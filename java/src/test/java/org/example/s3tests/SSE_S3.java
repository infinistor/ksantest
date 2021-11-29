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

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;


import org.junit.jupiter.api.DisplayName;
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

public class SSE_S3 extends TestBase{
    @Test
	@DisplayName("test_sse_s3_encrypted_transfer_1b") 
    @Tag( "PutGet") 
    //@Tag("1Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인") 
    public void test_sse_s3_encrypted_transfer_1b()
    {
        TestEncryptionSSES3ustomerWrite(1);
    }

    @Test
	@DisplayName("test_sse_s3_encrypted_transfer_1kb") 
    @Tag( "PutGet") 
    //@Tag("1KB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인") 
    public void test_sse_s3_encrypted_transfer_1kb()
    {
        TestEncryptionSSES3ustomerWrite(1024);
    }

    @Test
	@DisplayName("test_sse_s3_encrypted_transfer_1MB") 
    @Tag( "PutGet") 
    //@Tag("1MB 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인") 
    public void test_sse_s3_encrypted_transfer_1MB()
    {
        TestEncryptionSSES3ustomerWrite(1024 * 1024);
    }

    @Test
	@DisplayName("test_sse_s3_encrypted_transfer_13b") 
    @Tag( "PutGet") 
    //@Tag("13Byte 오브젝트를 SSE-S3 설정하여 업/다운로드가 올바르게 동작하는지 확인") 
    public void test_sse_s3_encrypted_transfer_13b()
    {
        TestEncryptionSSES3ustomerWrite(13);
    }

    @Test
	@DisplayName("test_sse_s3_encryption_method_head") 
    @Tag( "Metadata") 
    //@Tag("SSE-S3 설정하여 업로드한 오브젝트의 헤더정보읽기가 가능한지 확인") 
    public void test_sse_s3_encryption_method_head()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        var Key = "testobj";
        var Data = RandomTextToLong(1000);
        var Metadata = new ObjectMetadata();
		Metadata.addUserMetadata("x-amz-meta-foo", "bar");

        Metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

        Client.putObject(BucketName, Key, CreateBody(Data), Metadata);

        var Response = Client.getObjectMetadata(BucketName, Key);
        assertEquals(Metadata.getUserMetadata(), Response.getUserMetadata());
    }

    @Test
	@DisplayName("test_sse_s3_encryption_multipart_upload") 
    @Tag( "Multipart") 
    //@Tag("멀티파트업로드를 SSE-S3 설정하여 업로드 가능 확인") 
    public void test_sse_s3_encryption_multipart_upload()
    {
    	var BucketName = GetNewBucket();
        var Client = GetClient();
        var Key = "multipart_enc";
        var Size = 30 * MainData.MB;
        var ContentType = "text/plain";
        var MetadataList = new ObjectMetadata();
        MetadataList.addUserMetadata("x-amz-meta-foo", "bar");
        MetadataList.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        MetadataList.setContentType(ContentType);

        var InitMultiPartResponse = Client
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(BucketName, Key, MetadataList));
		var UploadID = InitMultiPartResponse.getUploadId();
		
		var Parts = GenerateRandomString(Size, 5 * MainData.MB);
		var PartETag = new ArrayList<PartETag>();
		int PartNumber = 1;
		var Data = "";
		for (var Part : Parts) {
			Data += Part;
			var PartResPonse = Client.uploadPart(new UploadPartRequest().withBucketName(BucketName).withKey(Key)
					.withUploadId(UploadID).withPartNumber(PartNumber++).withInputStream(CreateBody(Part))
					.withPartSize(Part.length()));
			PartETag.add(PartResPonse.getPartETag());
		}

		Client.completeMultipartUpload(new CompleteMultipartUploadRequest(BucketName, Key, UploadID, PartETag));
        var HeadResponse = Client.listObjectsV2(BucketName);
        var ObjectCount = HeadResponse.getKeyCount();
        assertEquals(1, ObjectCount);
        var BytesUsed = GetBytesUsed(HeadResponse);
        assertEquals(Size, BytesUsed);

        var GetResponse = Client.getObject(BucketName, Key);
        assertEquals(MetadataList.getUserMetadata(), GetResponse.getObjectMetadata().getUserMetadata());
        assertEquals(ContentType, GetResponse.getObjectMetadata().getContentType());
        
        var Body = GetBody(GetResponse.getObjectContent());
        assertEquals(Data, Body);
        assertEquals(Size, GetResponse.getObjectMetadata().getContentLength());

        CheckContentUsingRange(BucketName, Key, Data, 1000000);
        CheckContentUsingRange(BucketName, Key, Data, 10000000);
		CheckContentUsingRandomRange(BucketName, Key, Data, Size, 100);
    }

    @Test
	@DisplayName("test_get_bucket_encryption") 
    @Tag("encryption") 
    //@Tag("버킷의 SSE-S3 설정 확인") 
    public void test_get_bucket_encryption()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        
        assertThrows(AmazonS3Exception.class, () -> Client.getBucketEncryption(BucketName));
    }
    
    @Test
	@DisplayName("test_put_bucket_encryption") 
    @Tag("encryption") 
    @Tag("KSAN")
    //@Tag("버킷의 SSE-S3 설정이 가능한지 확인") 
    public void test_put_bucket_encryption()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        
        var SSES3Config = new ServerSideEncryptionConfiguration()
        		.withRules(new ServerSideEncryptionRule()
        				.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault().
        						withSSEAlgorithm(SSEAlgorithm.AES256)));
        
        Client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(BucketName).withServerSideEncryptionConfiguration(SSES3Config));
        
        // var Response = Client.getBucketEncryption(BucketName);
        // assertEquals(SSES3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());
    }
    
    @Test
	@DisplayName("test_delete_bucket_encryption") 
    @Tag("encryption") 
    //@Tag("버킷의 SSE-S3 설정 삭제가 가능한지 확인") 
    public void test_delete_bucket_encryption()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        
        var SSES3Config = new ServerSideEncryptionConfiguration()
        		.withRules(new ServerSideEncryptionRule()
        				.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault().
        						withSSEAlgorithm(SSEAlgorithm.AES256)));
        
        Client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(BucketName).withServerSideEncryptionConfiguration(SSES3Config));
        
        var Response = Client.getBucketEncryption(BucketName);
        assertEquals(1, Response.getServerSideEncryptionConfiguration().getRules().size());
        
        Client.deleteBucketEncryption(BucketName);
        
        assertThrows(AmazonS3Exception.class, () -> Client.getBucketEncryption(BucketName));
    }

    @Test
	@DisplayName("test_put_bucket_encryption_and_object_set_check") 
    @Tag("encryption") 
    //@Tag("버킷의 SSE-S3 설정이 오브젝트에 반영되는지 확인") 
    public void test_put_bucket_encryption_and_object_set_check()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        
        var SSES3Config = new ServerSideEncryptionConfiguration()
        		.withRules(new ServerSideEncryptionRule()
        				.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault().
        						withSSEAlgorithm(SSEAlgorithm.AES256)));
        
        Client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(BucketName).withServerSideEncryptionConfiguration(SSES3Config));
        
        var Response = Client.getBucketEncryption(BucketName);
        assertEquals(SSES3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());

		var KeyNames = new ArrayList<>(Arrays.asList(new String[] { "for/bar", "test/" }));
		CreateObjectsToBody(BucketName, KeyNames, "");

        var GetResponse = Client.getObject(BucketName, KeyNames.get(0));
        assertEquals(SSEAlgorithm.AES256.toString(), GetResponse.getObjectMetadata().getSSEAlgorithm());

        var GetHeadResponse = Client.getObjectMetadata(BucketName, KeyNames.get(1));
        assertEquals(SSEAlgorithm.AES256.toString(), GetHeadResponse.getSSEAlgorithm());
    }

    @Test
	@DisplayName("test_copy_object_encryption_1kb") 
    @Tag( "CopyObject") 
    //@Tag("버킷에 SSE-S3 설정하여 업로드한 1kb 오브젝트를 복사 가능한지 확인") 
    public void test_copy_object_encryption_1kb()
    {
        TestEncryptionSSES3Copy(1024);
    }

    @Test
	@DisplayName("test_copy_object_encryption_256kb") 
    @Tag( "CopyObject") 
    //@Tag("버킷에 SSE-S3 설정하여 업로드한 256kb 오브젝트를 복사 가능한지 확인") 
    public void test_copy_object_encryption_256kb()
    {
        TestEncryptionSSES3Copy(256 * 1024);
    }

    @Test
	@DisplayName("test_copy_object_encryption_1mb") 
    @Tag( "CopyObject") 
    //@Tag("버킷에 SSE-S3 설정하여 업로드한 1mb 오브젝트를 복사 가능한지 확인") 
    public void test_copy_object_encryption_1mb()
    {
        TestEncryptionSSES3Copy(1024 * 1024);
    }
    
    @Test
	@DisplayName("test_sse_s3_bucket_put_get") 
    @Tag( "PutGet") 
    //@Tag("[버킷에 SSE-S3 설정] 업로드, 다운로드 성공 확인") 
    public void test_sse_s3_bucket_put_get()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        var Data = RandomTextToLong(1000);
        
        var SSES3Config = new ServerSideEncryptionConfiguration()
        		.withRules(new ServerSideEncryptionRule()
        				.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault().
        						withSSEAlgorithm(SSEAlgorithm.AES256)));
        
        Client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(BucketName).withServerSideEncryptionConfiguration(SSES3Config));
        
        var Response = Client.getBucketEncryption(BucketName);
        assertEquals(SSES3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());

		var Key = "bar";
		Client.putObject(BucketName, Key, Data);

        var GetResponse = Client.getObject(BucketName, Key);
        var Body = GetBody(GetResponse.getObjectContent());
        assertEquals(SSEAlgorithm.AES256.toString(), GetResponse.getObjectMetadata().getSSEAlgorithm());
        assertEquals(Data, Body);
    }
    
    @Test
	@DisplayName("test_sse_s3_bucket_put_get_v4") 
    @Tag( "PutGet") 
    //@Tag("[버킷에 SSE-S3 설정, SignatureVersion4] 업로드, 다운로드 성공 확인") 
    public void test_sse_s3_bucket_put_get_v4()
    {
        var BucketName = GetNewBucket();
        var Client = GetClientV4(true);
        var Data = RandomTextToLong(1000);
        
        var SSES3Config = new ServerSideEncryptionConfiguration()
        		.withRules(new ServerSideEncryptionRule()
        				.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault().
        						withSSEAlgorithm(SSEAlgorithm.AES256)));
        
        Client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(BucketName).withServerSideEncryptionConfiguration(SSES3Config));
        
        var Response = Client.getBucketEncryption(BucketName);
        assertEquals(SSES3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());

		var Key = "bar";
		Client.putObject(BucketName, Key, Data);

        var GetResponse = Client.getObject(BucketName, Key);
        var Body = GetBody(GetResponse.getObjectContent());
        assertEquals(SSEAlgorithm.AES256.toString(), GetResponse.getObjectMetadata().getSSEAlgorithm());
        assertEquals(Data, Body);
    }
    
    @Test
	@DisplayName("test_sse_s3_bucket_put_get_use_chunk_encoding") 
    @Tag( "PutGet") 
    //@Tag("[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = true] 업로드, 다운로드 성공 확인") 
    public void test_sse_s3_bucket_put_get_use_chunk_encoding()
    {
        var BucketName = GetNewBucket();
        var Client = GetClientHttpsV4(true, false);
        var Data = RandomTextToLong(1000);
        
        var SSES3Config = new ServerSideEncryptionConfiguration()
        		.withRules(new ServerSideEncryptionRule()
        				.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault().
        						withSSEAlgorithm(SSEAlgorithm.AES256)));
        
        Client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(BucketName).withServerSideEncryptionConfiguration(SSES3Config));
        
        var Response = Client.getBucketEncryption(BucketName);
        assertEquals(SSES3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());

		var Key = "bar";
		Client.putObject(BucketName, Key, Data);

        var GetResponse = Client.getObject(BucketName, Key);
        var Body = GetBody(GetResponse.getObjectContent());
        assertEquals(SSEAlgorithm.AES256.toString(), GetResponse.getObjectMetadata().getSSEAlgorithm());
        assertEquals(Data, Body);
    }
    
    @Test
	@DisplayName("test_sse_s3_bucket_put_get_use_chunk_encoding_and_disable_payload_signing") 
    @Tag( "PutGet") 
    //@Tag("[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = true, DisablePayloadSigning = true] 업로드, 다운로드 성공 확인") 
    public void test_sse_s3_bucket_put_get_use_chunk_encoding_and_disable_payload_signing()
    {
        var BucketName = GetNewBucket();
        var Client = GetClientHttpsV4(true, true);
        var Data = RandomTextToLong(1000);
        
        var SSES3Config = new ServerSideEncryptionConfiguration()
        		.withRules(new ServerSideEncryptionRule()
        				.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault().
        						withSSEAlgorithm(SSEAlgorithm.AES256)));
        
        Client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(BucketName).withServerSideEncryptionConfiguration(SSES3Config));
        
        var Response = Client.getBucketEncryption(BucketName);
        assertEquals(SSES3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());

		var Key = "bar";
		Client.putObject(BucketName, Key, Data);

        var GetResponse = Client.getObject(BucketName, Key);
        var Body = GetBody(GetResponse.getObjectContent());
        assertEquals(SSEAlgorithm.AES256.toString(), GetResponse.getObjectMetadata().getSSEAlgorithm());
        assertEquals(Data, Body);
    }
    
    @Test
	@DisplayName("test_sse_s3_bucket_put_get_not_chunk_encoding") 
    @Tag( "PutGet") 
    //@Tag("[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = false] 업로드, 다운로드 성공 확인") 
    public void test_sse_s3_bucket_put_get_not_chunk_encoding()
    {
        var BucketName = GetNewBucket();
        var Client = GetClientHttpsV4(false, false);
        var Data = RandomTextToLong(1000);
        
        var SSES3Config = new ServerSideEncryptionConfiguration()
        		.withRules(new ServerSideEncryptionRule()
        				.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault().
        						withSSEAlgorithm(SSEAlgorithm.AES256)));
        
        Client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(BucketName).withServerSideEncryptionConfiguration(SSES3Config));
        
        var Response = Client.getBucketEncryption(BucketName);
        assertEquals(SSES3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());

		var Key = "bar";
		Client.putObject(BucketName, Key, Data);

        var GetResponse = Client.getObject(BucketName, Key);
        var Body = GetBody(GetResponse.getObjectContent());
        assertEquals(SSEAlgorithm.AES256.toString(), GetResponse.getObjectMetadata().getSSEAlgorithm());
        assertEquals(Data, Body);
    }
    
    @Test
	@DisplayName("test_sse_s3_bucket_put_get_not_chunk_encoding_and_disable_payload_signing") 
    @Tag( "PutGet") 
    //@Tag("[버킷에 SSE-S3 설정, SignatureVersion4, UseChunkEncoding = false, DisablePayloadSigning = true] 업로드, 다운로드 성공 확인") 
    public void test_sse_s3_bucket_put_get_not_chunk_encoding_and_disable_payload_signing()
    {
        var BucketName = GetNewBucket();
        var Client = GetClientHttpsV4(false, true);
        var Data = RandomTextToLong(1000);
        
        var SSES3Config = new ServerSideEncryptionConfiguration()
        		.withRules(new ServerSideEncryptionRule()
        				.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault().
        						withSSEAlgorithm(SSEAlgorithm.AES256)));
        
        Client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(BucketName).withServerSideEncryptionConfiguration(SSES3Config));
        
        var Response = Client.getBucketEncryption(BucketName);
        assertEquals(SSES3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());

		var Key = "bar";
		Client.putObject(BucketName, Key, Data);

        var GetResponse = Client.getObject(BucketName, Key);
        var Body = GetBody(GetResponse.getObjectContent());
        assertEquals(SSEAlgorithm.AES256.toString(), GetResponse.getObjectMetadata().getSSEAlgorithm());
        assertEquals(Data, Body);
    }

    @Test
	@DisplayName("test_sse_s3_bucket_presignedurl_put_get")
    @Tag("PresignedURL")
    //@Tag("[버킷에 SSE-S3 설정]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인")
    public void test_sse_s3_bucket_presignedurl_put_get()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        var Key = "foo";

        var SSES3Config = new ServerSideEncryptionConfiguration()
        		.withRules(new ServerSideEncryptionRule()
        				.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault().
        						withSSEAlgorithm(SSEAlgorithm.AES256)));
        
        Client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(BucketName).withServerSideEncryptionConfiguration(SSES3Config));
        
        var Response = Client.getBucketEncryption(BucketName);
        assertEquals(SSES3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());

        var PutURL = Client.generatePresignedUrl(BucketName, Key, GetTimeToAddSeconds(100000), HttpMethod.PUT);
        var PutResponse = PutObject(PutURL, Key);
        assertEquals(200, PutResponse.getStatusLine().getStatusCode());

        var GetURL = Client.generatePresignedUrl(BucketName, Key, GetTimeToAddSeconds(100000), HttpMethod.GET);
        var GetResponse = GetObject(GetURL);
        assertEquals(200, GetResponse.getStatusLine().getStatusCode());

    }

    @Test
	@DisplayName("test_sse_s3_bucket_presignedurl_put_get_v4")
    @Tag("PresignedURL")
    //@Tag("[버킷에 SSE-S3 설정, SignatureVersion4]PresignedURL로 오브젝트 업로드, 다운로드 성공 확인")
    public void test_sse_s3_bucket_presignedurl_put_get_v4()
    {
        var BucketName = GetNewBucket();
        var Client = GetClientV4(true);
        var Key = "foo";

        var SSES3Config = new ServerSideEncryptionConfiguration()
        		.withRules(new ServerSideEncryptionRule()
        				.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault().
        						withSSEAlgorithm(SSEAlgorithm.AES256)));
        
        Client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(BucketName).withServerSideEncryptionConfiguration(SSES3Config));
        
        var Response = Client.getBucketEncryption(BucketName);
        assertEquals(SSES3Config.getRules(), Response.getServerSideEncryptionConfiguration().getRules());

        var PutURL = Client.generatePresignedUrl(BucketName, Key, GetTimeToAddSeconds(100000), HttpMethod.PUT);
        var PutResponse = PutObject(PutURL, Key);
        assertEquals(200, PutResponse.getStatusLine().getStatusCode());

        var GetURL = Client.generatePresignedUrl(BucketName, Key, GetTimeToAddSeconds(100000), HttpMethod.GET);
        var GetResponse = GetObject(GetURL);
        assertEquals(200, GetResponse.getStatusLine().getStatusCode());
    }

    @Test
	@DisplayName("test_sse_s3_get_object_many")
    @Tag("Get")
    //@Tag("SSE-S3설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인")
    public void test_sse_s3_get_object_many()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        var Key = "foo";
        var Data = RandomTextToLong(15 * 1024 * 1024);

        var SSES3Config = new ServerSideEncryptionConfiguration()
        		.withRules(new ServerSideEncryptionRule()
        				.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault().
        						withSSEAlgorithm(SSEAlgorithm.AES256)));
        
        Client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(BucketName).withServerSideEncryptionConfiguration(SSES3Config));

        Client.putObject(BucketName, Key, Data);
        CheckContent(BucketName, Key, Data, 50);
    }
    
    @Test
	@DisplayName("test_sse_s3_range_object_many")
    @Tag("Get")
    //@Tag("SSE-S3설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인")
    public void test_sse_s3_range_object_many()
    {
        var BucketName = GetNewBucket();
        var Client = GetClient();
        var Key = "foo";
        var FileSize = 15 * 1024 * 1024;
        var Data = RandomTextToLong(FileSize);

        var SSES3Config = new ServerSideEncryptionConfiguration()
        		.withRules(new ServerSideEncryptionRule()
        				.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault().
        						withSSEAlgorithm(SSEAlgorithm.AES256)));
        
        Client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(BucketName).withServerSideEncryptionConfiguration(SSES3Config));

        Client.putObject(BucketName, Key, Data);
        CheckContentUsingRandomRange(BucketName, Key, Data, FileSize, 100);
    }
}
