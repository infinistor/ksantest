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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Base64;
import java.util.HashMap;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import org.example.s3tests.FormFile;
import org.example.s3tests.MainData;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class SSE_C extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll()
	{
		System.out.println("SSE_C Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll()
	{
		System.out.println("SSE_C End");
	}

	@Test
	@Tag( "PutGet")
	//1Byte 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_encrypted_transfer_1b()
	{
		TestEncryptionSSECustomerWrite(1);
	}

	@Test
	@Tag( "PutGet")
	//1KB 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_encrypted_transfer_1kb()
	{
		TestEncryptionSSECustomerWrite(1024);
	}

	@Test
	@Tag( "PutGet")
	//1MB 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_encrypted_transfer_1MB()
	{
		TestEncryptionSSECustomerWrite(1024 * 1024);
	}

	@Test
	@Tag( "PutGet")
	//13Byte 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_encrypted_transfer_13b()
	{
		TestEncryptionSSECustomerWrite(13);
	}

	@Test
	@Tag( "Metadata")
	//SSE-C 설정하여 업로드한 오브젝트를 SSE-C 설정하여 헤더정보읽기가 가능한지 확인
	public void test_encryption_sse_c_method_head()
	{
		var BucketName = GetNewBucket();
		var Client = GetClientHttps();
		var Key = "testobj";
		var Size = 1000;
		var Data = RandomTextToLong(Size);

		var Metadata = new ObjectMetadata();
		Metadata.setContentType("text/plain");
		Metadata.setContentLength(Size);

		var SSEC = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
		.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
		.withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		Client.putObject(new PutObjectRequest(BucketName, Key, CreateBody(Data), Metadata).withSSECustomerKey(SSEC));

		var e = assertThrows(AmazonServiceException.class, () -> Client.getObjectMetadata(BucketName, Key));
		var StatusCode = e.getStatusCode();
		assertEquals(400, StatusCode);

		Client.getObject(new GetObjectRequest(BucketName, Key).withSSECustomerKey(SSEC));
	}

	@Test
	@Tag( "ERROR")
	//SSE-C 설정하여 업로드한 오브젝트를 SSE-C 설정없이 다운로드 실패 확인
	public void test_encryption_sse_c_present()
	{
		var BucketName = GetNewBucket();
		var Client = GetClientHttps();
		var Key = "testobj";
		var Size = 1000;
		var Data = RandomTextToLong(Size);

		var Metadata = new ObjectMetadata();
		Metadata.setContentType("text/plain");
		Metadata.setContentLength(Size);

		var SSEC = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
		.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
		.withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		Client.putObject(new PutObjectRequest(BucketName, Key, CreateBody(Data), Metadata).withSSECustomerKey(SSEC));

		var e = assertThrows(AmazonServiceException.class, () -> Client.getObject(BucketName, Key));
		var StatusCode = e.getStatusCode();
		assertEquals(400, StatusCode);
	}

	@Test
	@Tag( "ERROR")
	//SSE-C 설정하여 업로드한 오브젝트와 다른 SSE-C 설정으로 다운로드 실패 확인
	public void test_encryption_sse_c_other_key()
	{
		var BucketName = GetNewBucket();
		var Client = GetClientHttps();
		var Key = "testobj";
		var Size = 100;
		var Data = RandomTextToLong(Size);

		var Metadata = new ObjectMetadata();
		Metadata.setContentType("text/plain");
		Metadata.setContentLength(Size);

		var SSEC_A = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
				.withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		var SSEC_B = new SSECustomerKey("6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=")
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
				.withMd5("arxBvwY2V4SiOne6yppVPQ==");

		Client.putObject(new PutObjectRequest(BucketName, Key, CreateBody(Data), Metadata).withSSECustomerKey(SSEC_A));

		var e = assertThrows(AmazonServiceException.class, () -> Client.getObject(new GetObjectRequest(BucketName, Key).withSSECustomerKey(SSEC_B)));
		var StatusCode = e.getStatusCode();
		assertEquals(403, StatusCode);
	}

	@Test
	@Tag( "ERROR")
	//SSE-C 설정값중 key-md5값이 올바르지 않을 경우 업로드 실패 확인
	public void test_encryption_sse_c_invalid_md5()
	{
		var BucketName = GetNewBucket();
		var Client = GetClientHttps();
		var Key = "testobj";
		var Size = 100;
		var Data = RandomTextToLong(Size);

		var Metadata = new ObjectMetadata();
		Metadata.setContentType("text/plain");
		Metadata.setContentLength(Size);

		var SSEC = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
		.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
		.withMd5("AAAAAAAAAAAAAAAAAAAAAA==");

		var e = assertThrows(AmazonServiceException.class, () -> Client.putObject(new PutObjectRequest(BucketName, Key, CreateBody(Data), Metadata).withSSECustomerKey(SSEC)));

		var StatusCode = e.getStatusCode();
		assertEquals(400, StatusCode);
	}


	@Test
	@Tag( "ERROR")
	//SSE-C 설정값중 key-md5값을 누락했을 경우 업로드 성공 확인
	public void test_encryption_sse_c_no_md5()
	{
		var BucketName = GetNewBucket();
		var Client = GetClientHttps();
		var Key = "testobj";
		var Size = 100;
		var Data = RandomTextToLong(Size);

		var Metadata = new ObjectMetadata();
		Metadata.setContentType("text/plain");
		Metadata.setContentLength(Size);

		var SSEC = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
		.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

		Client.putObject(new PutObjectRequest(BucketName, Key, CreateBody(Data), Metadata).withSSECustomerKey(SSEC));
		var Response = Client.getObject(new GetObjectRequest(BucketName, Key).withSSECustomerKey(SSEC));
		var Body = GetBody(Response.getObjectContent());
		assertEquals(Data, Body);
	}

	@Test
	@Tag( "ERROR")
	//SSE-C 설정값중 key값을 누락했을 경우 업로드 실패 확인
	public void test_encryption_sse_c_no_key()
	{
		assertThrows(IllegalArgumentException.class, () -> new SSECustomerKey("").withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION));
	}

	@Test
	@Disabled("JAVA 에서는 algorithm값을 누락해도 기본값이 지정되어 있어 에러가 발생하지 않음")
	@Tag( "ERROR")
	// @Tag("SSE-C 설정값중 algorithm값을 누락했을 경우 업로드 실패 확인
	public void test_encryption_key_no_sse_c()
	{
		var BucketName = GetNewBucket();
		var Client = GetClientHttps();
		var Key = "testobj";
		var Size = 100;
		var Data = RandomTextToLong(Size);

		var Metadata = new ObjectMetadata();
		Metadata.setContentType("text/plain");
		Metadata.setContentLength(Size);

		var SSEC = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
		.withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		var e = assertThrows(AmazonServiceException.class, () -> Client.putObject(new PutObjectRequest(BucketName, Key, CreateBody(Data), Metadata).withSSECustomerKey(SSEC)));
		var StatusCode = e.getStatusCode();
		assertEquals(400, StatusCode);
	}

	@Test
	@Tag( "Multipart")
	//멀티파트업로드를 SSE-C 설정하여 업로드 가능 확인
	public void test_encryption_sse_c_multipart_upload()
	{
		var BucketName = GetNewBucket();
		var Client = GetClientHttps();
		var Key = "multipart_enc";
		var Size = 50 * MainData.MB;
		var ContentType = "text/plain";
		var Metadata = new ObjectMetadata();
		Metadata.addUserMetadata("x-amz-meta-foo", "bar");
		Metadata.setContentType(ContentType);
		var SSEC = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
		.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
		.withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		var UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, 0, Metadata, SSEC);

		Client.completeMultipartUpload(new CompleteMultipartUploadRequest(BucketName, Key, UploadData.UploadId, UploadData.Parts));

		var HeadResponse = Client.listObjectsV2(BucketName);
		var ObjectCount = HeadResponse.getKeyCount();
		assertEquals(1, ObjectCount);
		var BytesUsed = GetBytesUsed(HeadResponse);
		assertEquals(Size, BytesUsed);

		var GetResponse = Client.getObject(new GetObjectRequest(BucketName, Key).withSSECustomerKey(SSEC));
		assertEquals(Metadata.getUserMetadata(), GetResponse.getObjectMetadata().getUserMetadata());
		assertEquals(ContentType, GetResponse.getObjectMetadata().getContentType());

		var Body = UploadData.GetBody();
		CheckContentUsingRangeEnc(Client, BucketName, Key, Body, MainData.MB, SSEC);
		CheckContentUsingRangeEnc(Client, BucketName, Key, Body, 10 * MainData.MB, SSEC);
		CheckContentUsingRandomRangeEnc(Client, BucketName, Key, Body, Size, 100, SSEC);
	}


	@Test
	@Tag( "Multipart")
	//SSE-C 설정하여 멀티파트 업로드한 오브젝트와 다른 SSE-C 설정으로 다운로드 실패 확인
	public void test_encryption_sse_c_multipart_bad_download()
	{
		var BucketName = GetNewBucket();
		var Client = GetClientHttps();
		var Key = "multipart_enc";
		var Size = 50 * MainData.MB;
		var ContentType = "text/plain";
		var Metadata = new ObjectMetadata();
		Metadata.addUserMetadata("x-amz-meta-foo", "bar");
		Metadata.setContentType(ContentType);

		var SetSSEC = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
				.withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		var GetSSEC = new SSECustomerKey("6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=")
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
				.withMd5("arxBvwY2V4SiOne6yppVPQ==");

		var UploadData = SetupMultipartUpload(Client, BucketName, Key, Size, 0, Metadata, SetSSEC);

		Client.completeMultipartUpload(new CompleteMultipartUploadRequest(BucketName, Key, UploadData.UploadId, UploadData.Parts));

		var HeadResponse = Client.listObjectsV2(BucketName);
		var ObjectCount = HeadResponse.getKeyCount();
		assertEquals(1, ObjectCount);
		var BytesUsed = GetBytesUsed(HeadResponse);
		assertEquals(Size, BytesUsed);

		var GetResponse = Client.getObject(new GetObjectRequest(BucketName, Key).withSSECustomerKey(SetSSEC));
		assertEquals(Metadata.getUserMetadata(), GetResponse.getObjectMetadata().getUserMetadata());
		assertEquals(ContentType, GetResponse.getObjectMetadata().getContentType());

		var e = assertThrows(AmazonServiceException.class, ()-> Client.getObject(new GetObjectRequest(BucketName, Key).withSSECustomerKey(GetSSEC)));
		var StatusCode = e.getStatusCode();
		assertEquals(403, StatusCode);
	}


	@Test
	@Tag( "Post")
	//Post 방식으로 SSE-C 설정하여 오브젝트 업로드가 올바르게 동작하는지 확인
	public void test_encryption_sse_c_post_object_authenticated_request()
	{
		var BucketName = GetNewBucket();
		var Client = GetClientHttps();

		var ContentType = "text/plain";
		var Key = "foo.txt";
		var PolicyDocument = new JsonObject();
		PolicyDocument.addProperty("expiration", GetTimeToAddMinutes(100));

		var Conditions = new JsonArray();

		var Bucket = new JsonObject();
		Bucket.addProperty("bucket", BucketName);
		Conditions.add(Bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("foo");
		Conditions.add(starts1);

		var ACL = new JsonObject();
		ACL.addProperty("acl", "private");
		Conditions.add(ACL);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(ContentType);
		Conditions.add(starts2);


		var starts3 = new JsonArray();
		starts3.add("starts-with");
		starts3.add("$x-amz-server-side-encryption-customer-algorithm");
		starts3.add("");
		Conditions.add(starts3);

		var starts4 = new JsonArray();
		starts4.add("starts-with");
		starts4.add("$x-amz-server-side-encryption-customer-key");
		starts4.add("");
		Conditions.add(starts4);

		var starts5 = new JsonArray();
		starts5.add("starts-with");
		starts5.add("$x-amz-server-side-encryption-customer-key-md5");
		starts5.add("");
		Conditions.add(starts5);

		var ContentLengthRange = new JsonArray();
		ContentLengthRange.add("content-length-range");
		ContentLengthRange.add(0);
		ContentLengthRange.add(1024);
		Conditions.add(ContentLengthRange);

		PolicyDocument.add("conditions", Conditions);

		var BytesJsonPolicyDocument = PolicyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var Policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = GetBase64EncodedSHA1Hash(Policy, Config.MainUser.SecretKey);
		var FileData = new FormFile(Key, ContentType, "bar");
		var Payload = new HashMap<String, String>();
		Payload.put( "key", Key );
		Payload.put( "AWSAccessKeyId", Config.MainUser.AccessKey );
		Payload.put( "acl", "private" );
		Payload.put( "signature", Signature );
		Payload.put( "policy", Policy );
		Payload.put( "Content-Type", ContentType );
		Payload.put( "x-amz-server-side-encryption-customer-algorithm", "AES256" );
		Payload.put( "x-amz-server-side-encryption-customer-key", "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=" );
		Payload.put( "x-amz-server-side-encryption-customer-key-md5", "DWygnHRtgiJ77HCm+1rvHw==" );

		var Result = PostUploadHttps(BucketName, Payload, FileData);
		assertEquals(204, Result.StatusCode);

		var Response = Client.getObject(new GetObjectRequest(BucketName, Key).withSSECustomerKey(new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")));
		var Body = GetBody(Response.getObjectContent());
		assertEquals("bar", Body);
	}

	@Test
	@Tag("Get")
	//SSE-C설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인
	public void test_encryption_sse_c_get_object_many()
	{
		var BucketName = GetNewBucket();
		var Client = GetClientHttps();
		var Key = "testobj";
		var Size = 15 * 1024 * 1024;
		var Data = RandomTextToLong(Size);

		var Metadata = new ObjectMetadata();
		Metadata.setContentType("text/plain");
		Metadata.setContentLength(Size);

		var SSEC = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=").
					   withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION).
					   withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		Client.putObject(new PutObjectRequest(BucketName, Key, CreateBody(Data), Metadata).withSSECustomerKey(SSEC));
		CheckContentEnc(BucketName, Key, Data, 50, SSEC);
	}

	@Test
	@Tag("Get")
	//SSE-C설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
	public void test_encryption_sse_c_range_object_many()
	{
		var BucketName = GetNewBucket();
		var Client = GetClientHttps();
		var Key = "testobj";
		var Size = 15 * 1024 * 1024;
		var Data = RandomTextToLong(Size);

		var Metadata = new ObjectMetadata();
		Metadata.setContentType("text/plain");
		Metadata.setContentLength(Size);

		var SSEC = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=").
					   withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION).
					   withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		Client.putObject(new PutObjectRequest(BucketName, Key, CreateBody(Data), Metadata).withSSECustomerKey(SSEC));

		var Response = Client.getObject(new GetObjectRequest(BucketName, Key).withSSECustomerKey(SSEC));
		var Body = GetBody(Response.getObjectContent());
		assertTrue(Data.equals(Body), "Source does not match target");

		CheckContentUsingRandomRangeEnc(Client, BucketName, Key, Data, Size, 50, SSEC);
	}

	@Test
	@Tag( "Multipart")
	//SSE-C 설정하여 멀티파트로 업로드한 오브젝트를 mulitcopy 로 복사 가능한지 확인
	public void test_sse_c_encryption_multipart_copypart_upload()
	{
		var BucketName = GetNewBucket();
		var Client = GetClientHttps();
		var SourceKey = "multipart_enc";
		var Size = 50 * MainData.MB;
		var ContentType = "text/plain";

		var Metadata = new ObjectMetadata();
		Metadata.setContentType("text/plain");
		Metadata.setContentLength(Size);

		var SSEC = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=").
					   withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION).
					   withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		var UploadData = SetupMultipartUpload(Client, BucketName, SourceKey, Size, 0, Metadata, SSEC);

		Client.completeMultipartUpload(new CompleteMultipartUploadRequest(BucketName, SourceKey, UploadData.UploadId, UploadData.Parts));

		var HeadResponse = Client.listObjectsV2(BucketName);
		var ObjectCount = HeadResponse.getKeyCount();
		assertEquals(1, ObjectCount);
		var BytesUsed = GetBytesUsed(HeadResponse);
		assertEquals(Size, BytesUsed);

		var GetResponse = Client.getObject(new GetObjectRequest(BucketName, SourceKey).withSSECustomerKey(SSEC));
		assertEquals(Metadata.getUserMetadata(), GetResponse.getObjectMetadata().getUserMetadata());
		assertEquals(ContentType, GetResponse.getObjectMetadata().getContentType());

		// 멀티파트 복사
		var TargetKey = "multipart_enc_copy";
		UploadData = MultipartCopySSEC(Client, BucketName, SourceKey, BucketName, TargetKey, Size, Metadata, SSEC);
		Client.completeMultipartUpload(new CompleteMultipartUploadRequest(BucketName, TargetKey, UploadData.UploadId, UploadData.Parts));
		CheckCopyContentSSEC(Client, BucketName, SourceKey, BucketName, TargetKey, SSEC);
	}

	@Test
	@Tag( "Multipart")
	//SSE-C 설정하여 Multipart와 Copypart를 모두 사용하여 오브젝트가 업로드 가능한지 확인
	public void test_sse_c_encryption_multipart_copy_many()
	{
		var BucketName = GetNewBucket();
		var Client = GetClientHttps();
		var SourceKey = "multipart_enc";
		var Size = 50 * MainData.MB;
		var ContentType = "text/plain";
		var Metadata = new ObjectMetadata();
		Metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
		Metadata.setContentType(ContentType);
		var Body = new StringBuilder();

		// 멀티파트 업로드
		var UploadData = SetupMultipartUpload(Client, BucketName, SourceKey, Size, Metadata);
		Client.completeMultipartUpload(new CompleteMultipartUploadRequest(BucketName, SourceKey, UploadData.UploadId, UploadData.Parts));

		// 업로드가 올바르게 되었는지 확인
		Body.append(UploadData.Body);
		CheckContentUsingRange(BucketName, SourceKey, Body.toString(), MainData.MB);

		// 멀티파트 카피
		var TargetKey1 = "mymultipart1";
		UploadData = MultipartCopy(Client, BucketName, SourceKey, BucketName, TargetKey1, Size, Metadata);
		// 추가파츠 업로드
		UploadData = MultipartUpload(Client, BucketName, TargetKey1, Size, UploadData);
		Client.completeMultipartUpload(new CompleteMultipartUploadRequest(BucketName, TargetKey1, UploadData.UploadId, UploadData.Parts));

		// 업로드가 올바르게 되었는지 확인
		Body.append(UploadData.Body);
		CheckContentUsingRange(BucketName, TargetKey1, Body.toString(), MainData.MB);

		// 멀티파트 카피
		var TargetKey2 = "mymultipart2";
		UploadData = MultipartCopy(Client, BucketName, TargetKey1, BucketName, TargetKey2, Size * 2, Metadata);
		// 추가파츠 업로드
		UploadData = MultipartUpload(Client, BucketName, TargetKey2, Size, UploadData);
		Client.completeMultipartUpload(new CompleteMultipartUploadRequest(BucketName, TargetKey2, UploadData.UploadId, UploadData.Parts));

		// 업로드가 올바르게 되었는지 확인
		Body.append(UploadData.Body);
		CheckContentUsingRange(BucketName, TargetKey2, Body.toString(), MainData.MB);
	}
}
