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
package org.example.test;

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
	static public void BeforeAll()
	{
		System.out.println("SSE_S3 Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll()
	{
		System.out.println("SSE_S3 End");
	}

	@Test
	@Tag("PutGet")
	//1Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_sse_s3_encrypted_transfer_1b()
	{
		TestEncryptionSSE_S3CustomerWrite(1);
	}

	@Test
	@Tag("PutGet")
	//1KB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_sse_s3_encrypted_transfer_1kb()
	{
		TestEncryptionSSE_S3CustomerWrite(1024);
	}

	@Test
	@Tag("PutGet")
	//1MB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_sse_s3_encrypted_transfer_1MB()
	{
		TestEncryptionSSE_S3CustomerWrite(1024 * 1024);
	}

	@Test
	@Tag("PutGet")
	//13Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_sse_s3_encrypted_transfer_13b()
	{
		TestEncryptionSSE_S3CustomerWrite(13);
	}

	@Test
	@Tag("Metadata")
	//SSE-S3 설정하여 업로드한 오브젝트의 헤더정보읽기가 가능한지 확인
	public void test_sse_s3_encryption_method_head()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var Key = "testobj";
		var Data = Utils.randomTextToLong(1000);
		var Metadata = new ObjectMetadata();
		Metadata.addUserMetadata("x-amz-meta-foo", "bar");

		Metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

		client.putObject(bucketName, Key, createBody(Data), Metadata);

		var Response = client.getObjectMetadata(bucketName, Key);
		assertEquals(Metadata.getUserMetadata(), Response.getUserMetadata());
	}

	@Test
	@Tag("Multipart")
	//멀티파트업로드를 SSE-S3 설정하여 업로드 가능 확인
	public void test_sse_s3_encryption_multipart_upload()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var Key = "multipart_enc";
		var Size = 50 * MainData.MB;
		var ContentType = "text/plain";
		var MetadataList = new ObjectMetadata();
		MetadataList.addUserMetadata("x-amz-meta-foo", "bar");
		MetadataList.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		MetadataList.setContentType(ContentType);

		var InitMultiPartResponse = client
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, Key, MetadataList));
		var UploadID = InitMultiPartResponse.getUploadId();

		var Parts = Utils.generateRandomString(Size, 5 * MainData.MB);
		var PartETag = new ArrayList<PartETag>();
		int PartNumber = 1;
		var Data = "";
		for (var Part : Parts) {
			Data += Part;
			var PartResPonse = client.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(Key)
					.withUploadId(UploadID).withPartNumber(PartNumber++).withInputStream(createBody(Part))
					.withPartSize(Part.length()));
			PartETag.add(PartResPonse.getPartETag());
		}

		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, Key, UploadID, PartETag));
		var HeadResponse = client.listObjectsV2(bucketName);
		var ObjectCount = HeadResponse.getKeyCount();
		assertEquals(1, ObjectCount);
		var BytesUsed = GetBytesUsed(HeadResponse);
		assertEquals(Size, BytesUsed);

		var GetResponse = client.getObjectMetadata(bucketName, Key);
		assertEquals(MetadataList.getUserMetadata(), GetResponse.getUserMetadata());
		assertEquals(ContentType, GetResponse.getContentType());

		CheckContentUsingRange(bucketName, Key, Data, MainData.MB);
		CheckContentUsingRange(bucketName, Key, Data, 10 * MainData.MB);
		CheckContentUsingRandomRange(bucketName, Key, Data, 100);
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

		var SSES3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSES3Config));

		// var Response = client.getBucketEncryption(bucketName);
		// assertEquals(SSES3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());
	}

	@Test
	@Tag("encryption")
	//버킷의 SSE-S3 설정 삭제가 가능한지 확인
	public void test_delete_bucket_encryption()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		var SSES3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSES3Config));

		var Response = client.getBucketEncryption(bucketName);
		assertEquals(1, Response.getServerSideEncryptionConfiguration().getRules().size());

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

		var SSES3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSES3Config));

		var Response = client.getBucketEncryption(bucketName);
		assertEquals(SSES3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());

		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "for/bar", "test/" }));
		createObjectsToBody(bucketName, KeyNames, "");

		var GetHeadResponse = client.getObjectMetadata(bucketName, KeyNames.get(1));
		assertEquals(SSEAlgorithm.AES256.toString(), GetHeadResponse.getSSEAlgorithm());
	}

	@Test
	@Tag("CopyObject")
	//버킷에 SSE-S3 설정하여 업로드한 1kb 오브젝트를 복사 가능한지 확인
	public void test_copy_object_encryption_1kb()
	{
		TestEncryptionSSE_S3Copy(1024);
	}

	@Test
	@Tag("CopyObject")
	//버킷에 SSE-S3 설정하여 업로드한 256kb 오브젝트를 복사 가능한지 확인
	public void test_copy_object_encryption_256kb()
	{
		TestEncryptionSSE_S3Copy(256 * 1024);
	}

	@Test
	@Tag("CopyObject")
	//버킷에 SSE-S3 설정하여 업로드한 1mb 오브젝트를 복사 가능한지 확인
	public void test_copy_object_encryption_1mb()
	{
		TestEncryptionSSE_S3Copy(1024 * 1024);
	}

	@Test
	@Tag("PutGet")
	//[버킷에 SSE-S3 설정] 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_put_get()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var Data = Utils.randomTextToLong(1000);

		var SSES3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSES3Config));

		var Response = client.getBucketEncryption(bucketName);
		assertEquals(SSES3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());

		var Key = "bar";
		client.putObject(bucketName, Key, Data);

		var GetResponse = client.getObject(bucketName, Key);
		var Body = GetBody(GetResponse.getObjectContent());
		assertEquals(SSEAlgorithm.AES256.toString(), GetResponse.getObjectMetadata().getSSEAlgorithm());
		assertTrue(Data.equals(Body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PutGet")
	//[버킷에 SSE-S3 설정, SignatureVersion4] 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_put_get_v4()
	{
		var bucketName = getNewBucket();
		var client = getClientV4(true);
		var Data = Utils.randomTextToLong(1000);

		var SSES3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSES3Config));

		var Response = client.getBucketEncryption(bucketName);
		assertEquals(SSES3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());

		var Key = "bar";
		client.putObject(bucketName, Key, Data);

		var GetResponse = client.getObject(bucketName, Key);
		var Body = GetBody(GetResponse.getObjectContent());
		assertEquals(SSEAlgorithm.AES256.toString(), GetResponse.getObjectMetadata().getSSEAlgorithm());
		assertTrue(Data.equals(Body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PutGet")
	//[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = true] 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_put_get_use_chunk_encoding()
	{
		var bucketName = getNewBucket();
		var client = getClientHttpsV4(true, false);
		var Data = Utils.randomTextToLong(1000);

		var SSES3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSES3Config));

		var Response = client.getBucketEncryption(bucketName);
		assertEquals(SSES3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());

		var Key = "bar";
		client.putObject(bucketName, Key, Data);

		var GetResponse = client.getObject(bucketName, Key);
		var Body = GetBody(GetResponse.getObjectContent());
		assertEquals(SSEAlgorithm.AES256.toString(), GetResponse.getObjectMetadata().getSSEAlgorithm());
		assertTrue(Data.equals(Body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PutGet")
	//[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = true, DisablePayloadSigning = true] 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_put_get_use_chunk_encoding_and_disable_payload_signing()
	{
		var bucketName = getNewBucket();
		var client = getClientHttpsV4(true, true);
		var Data = Utils.randomTextToLong(1000);

		var SSES3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSES3Config));

		var Response = client.getBucketEncryption(bucketName);
		assertEquals(SSES3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());

		var Key = "bar";
		client.putObject(bucketName, Key, Data);

		var GetResponse = client.getObject(bucketName, Key);
		var Body = GetBody(GetResponse.getObjectContent());
		assertEquals(SSEAlgorithm.AES256.toString(), GetResponse.getObjectMetadata().getSSEAlgorithm());
		assertTrue(Data.equals(Body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PutGet")
	//[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = false] 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_put_get_not_chunk_encoding()
	{
		var bucketName = getNewBucket();
		var client = getClientHttpsV4(false, false);
		var Data = Utils.randomTextToLong(1000);

		var SSES3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSES3Config));

		var Response = client.getBucketEncryption(bucketName);
		assertEquals(SSES3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());

		var Key = "bar";
		client.putObject(bucketName, Key, Data);

		var GetResponse = client.getObject(bucketName, Key);
		var Body = GetBody(GetResponse.getObjectContent());
		assertEquals(SSEAlgorithm.AES256.toString(), GetResponse.getObjectMetadata().getSSEAlgorithm());
		assertTrue(Data.equals(Body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PutGet")
	//[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = false, DisablePayloadSigning = true] 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_put_get_not_chunk_encoding_and_disable_payload_signing()
	{
		var bucketName = getNewBucket();
		var client = getClientHttpsV4(false, true);
		var Data = Utils.randomTextToLong(1000);

		var SSES3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSES3Config));

		var Response = client.getBucketEncryption(bucketName);
		assertEquals(SSES3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());

		var Key = "bar";
		client.putObject(bucketName, Key, Data);

		var GetResponse = client.getObject(bucketName, Key);
		var Body = GetBody(GetResponse.getObjectContent());
		assertEquals(SSEAlgorithm.AES256.toString(), GetResponse.getObjectMetadata().getSSEAlgorithm());
		assertTrue(Data.equals(Body), MainData.NOT_MATCHED);
	}

	@Test
	@Tag("PresignedURL")
	//[버킷에 SSE-S3 설정]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_presignedurl_put_get()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var Key = "foo";

		var SSES3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSES3Config));

		var Response = client.getBucketEncryption(bucketName);
		assertEquals(SSES3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());

		var PutURL = client.generatePresignedUrl(bucketName, Key, getTimeToAddSeconds(100000), HttpMethod.PUT);
		var PutResponse = PutObject(PutURL, Key);
		assertEquals(200, PutResponse.getStatusLine().getStatusCode());

		var GetURL = client.generatePresignedUrl(bucketName, Key, getTimeToAddSeconds(100000), HttpMethod.GET);
		var GetResponse = GetObject(GetURL);
		assertEquals(200, GetResponse.getStatusLine().getStatusCode());

	}

	@Test
	@Tag("PresignedURL")
	//[버킷에 SSE-S3 설정, SignatureVersion4]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인
	public void test_sse_s3_bucket_presignedurl_put_get_v4()
	{
		var bucketName = getNewBucket();
		var client = getClientV4(true);
		var Key = "foo";

		var SSES3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSES3Config));

		var Response = client.getBucketEncryption(bucketName);
		assertEquals(SSES3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());

		var PutURL = client.generatePresignedUrl(bucketName, Key, getTimeToAddSeconds(100000), HttpMethod.PUT);
		var PutResponse = PutObject(PutURL, Key);
		assertEquals(200, PutResponse.getStatusLine().getStatusCode());

		var GetURL = client.generatePresignedUrl(bucketName, Key, getTimeToAddSeconds(100000), HttpMethod.GET);
		var GetResponse = GetObject(GetURL);
		assertEquals(200, GetResponse.getStatusLine().getStatusCode());
	}

	@Test
	@Tag("Get")
	//SSE-S3설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인
	public void test_sse_s3_get_object_many()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var Key = "foo";
		var Data = Utils.randomTextToLong(15 * MainData.MB);

		var SSES3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSES3Config));

		client.putObject(bucketName, Key, Data);
		CheckContent(bucketName, Key, Data, 50);
	}

	@Test
	@Tag("Get")
	//SSE-S3설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
	public void test_sse_s3_range_object_many()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var Key = "foo";
		var FileSize = 15 * 1024 * 1024;
		var Data = Utils.randomTextToLong(FileSize);

		var SSES3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSES3Config));

		client.putObject(bucketName, Key, Data);
		CheckContentUsingRandomRange(bucketName, Key, Data, 100);
	}

	@Test
	@Tag("Multipart")
	//SSE-S3 설정하여 멀티파트로 업로드한 오브젝트를 mulitcopy 로 복사 가능한지 확인
	public void test_sse_s3_encryption_multipart_copypart_upload()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var SourceKey = "multipart_enc";
		var Size = 50 * MainData.MB;
		var ContentType = "text/plain";
		var Metadata = new ObjectMetadata();
		Metadata.addUserMetadata("x-amz-meta-foo", "bar");
		Metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		Metadata.setContentType(ContentType);

		// 멀티파트 업로드
		var UploadData = SetupMultipartUpload(client, bucketName, SourceKey, Size, Metadata);
		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, SourceKey, UploadData.uploadId, UploadData.parts));

		//올바르게 업로드 되었는지 확인
		var HeadResponse = client.listObjectsV2(bucketName);
		var ObjectCount = HeadResponse.getKeyCount();
		assertEquals(1, ObjectCount);
		var BytesUsed = GetBytesUsed(HeadResponse);
		assertEquals(Size, BytesUsed);

		var GetResponse = client.getObjectMetadata(bucketName, SourceKey);
		assertEquals(Metadata.getUserMetadata(), GetResponse.getUserMetadata());
		assertEquals(ContentType, GetResponse.getContentType());

		CheckContentUsingRange(bucketName, SourceKey, UploadData.getBody(), MainData.MB);

		// 멀티파트 복사
		var TargetKey = "multipart_enc_copy";
		UploadData = MultipartCopy(client, bucketName, SourceKey, bucketName, TargetKey, Size, Metadata);
		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, TargetKey, UploadData.uploadId, UploadData.parts));

		//올바르게 복사 되었는지 확인
		CheckCopyContentUsingRange(bucketName, SourceKey, bucketName, TargetKey, MainData.MB);
	}

	@Test
	@Tag("Multipart")
	//SSE-S3 설정하여 Multipart와 Copypart를 모두 사용하여 오브젝트가 업로드 가능한지 확인
	public void test_sse_s3_encryption_multipart_copy_many()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var SourceKey = "multipart_enc";
		var Size = 10 * MainData.MB;
		var ContentType = "text/plain";
		var Metadata = new ObjectMetadata();
		Metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		Metadata.setContentType(ContentType);
		var Body = new StringBuilder();

		// 멀티파트 업로드
		var UploadData = SetupMultipartUpload(client, bucketName, SourceKey, Size, Metadata);
		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, SourceKey, UploadData.uploadId, UploadData.parts));

		// 업로드가 올바르게 되었는지 확인
		Body.append(UploadData.body);
		CheckContentUsingRange(bucketName, SourceKey, Body.toString(), MainData.MB);

		// 멀티파트 카피
		var TargetKey1 = "mymultipart1";
		UploadData = MultipartCopy(client, bucketName, SourceKey, bucketName, TargetKey1, Size, Metadata);
		// 추가파츠 업로드
		UploadData = MultipartUpload(client, bucketName, TargetKey1, Size, UploadData);
		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, TargetKey1, UploadData.uploadId, UploadData.parts));

		// 업로드가 올바르게 되었는지 확인
		Body.append(UploadData.body);
		CheckContentUsingRange(bucketName, TargetKey1, Body.toString(), MainData.MB);

		// 멀티파트 카피
		var TargetKey2 = "mymultipart2";
		UploadData = MultipartCopy(client, bucketName, TargetKey1, bucketName, TargetKey2, Size * 2, Metadata);
		// 추가파츠 업로드
		UploadData = MultipartUpload(client, bucketName, TargetKey2, Size, UploadData);
		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, TargetKey2, UploadData.uploadId, UploadData.parts));

		// 업로드가 올바르게 되었는지 확인
		Body.append(UploadData.body);
		CheckContentUsingRange(bucketName, TargetKey2, Body.toString(), MainData.MB);
	}
	
	@Test
	@Tag("Retroactive")
	//sse-s3설정은 소급적용 되지 않음을 확인
	public void test_sse_s3_not_retroactive()
	{
		var bucketName = getNewBucket();
		var client = getClient();
		var Data = Utils.randomTextToLong(1000);
		
		var Key = "bar";
		client.putObject(bucketName, Key, Data);

		var SSES3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));

		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(bucketName).withServerSideEncryptionConfiguration(SSES3Config));

		var Response = client.getBucketEncryption(bucketName);
		assertEquals(SSES3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());

		var GetResponse = client.getObject(bucketName, Key);
		var Body = GetBody(GetResponse.getObjectContent());
		assertTrue(Data.equals(Body), MainData.NOT_MATCHED);

		
		var Key2 = "bar2";
		var Data2 = Utils.randomTextToLong(1000);
		client.putObject(bucketName, Key2, Data2);
		client.deleteBucketEncryption(bucketName);

		GetResponse = client.getObject(bucketName, Key2);
		Body = GetBody(GetResponse.getObjectContent());
		assertTrue(Data2.equals(Body), MainData.NOT_MATCHED);
	}
}
