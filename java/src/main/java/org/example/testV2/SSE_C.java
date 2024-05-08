/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License. See LICENSE for details
*
* 본 프로그램 및 관련 소스코드, 문서 등 모든 자료는 있는 그대로 제공이 됩니다.
* KSAN 프로젝트의 개발자 및 개발사는 이 프로그램을 사용한 결과에 따른 어떠한 책임도 지지 않습니다.
* KSAN 개발팀은 사전 공지, 허락, 동의 없이 KSAN 개발에 관련된 모든 결과물에 대한 LICENSE 방식을 변경 할 권리가 있습니다.
*/
package org.example.testV2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.net.MalformedURLException;
import java.util.Base64;
import java.util.HashMap;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import org.example.Data.FormFile;
import org.example.Data.MainData;
import org.example.Utility.NetUtils;
import org.example.Utility.Utils;
import org.example.auth.AWS2SignerBase;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class SSE_C extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll()
	{
		System.out.println("SSE_C SDK V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll()
	{
		System.out.println("SSE_C SDK V2 End");
	}

	@Test
	@Tag("PutGet")
	//1Byte 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_encrypted_transfer_1b()
	{
		testEncryptionSSECustomerWrite(1);
	}

	@Test
	@Tag("PutGet")
	//1KB 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_encrypted_transfer_1kb()
	{
		testEncryptionSSECustomerWrite(1024);
	}

	@Test
	@Tag("PutGet")
	//1MB 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_encrypted_transfer_1MB()
	{
		testEncryptionSSECustomerWrite(1024 * 1024);
	}

	@Test
	@Tag("PutGet")
	//13Byte 오브젝트를 SSE-C 설정하여 업/다운로드가 올바르게 동작하는지 확인
	public void test_encrypted_transfer_13b()
	{
		testEncryptionSSECustomerWrite(13);
	}

	@Test
	@Tag("metadata")
	//SSE-C 설정하여 업로드한 오브젝트를 SSE-C 설정하여 헤더정보읽기가 가능한지 확인
	public void test_encryption_sse_c_method_head()
	{
		var bucketName = getNewBucket();
		var client = getClientHttps();
		var key = "obj";
		var size = 1000;
		var data = Utils.randomTextToLong(size);

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(size);

		var SSE_C = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
		.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
		.withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		client.putObject(new PutObjectRequest(bucketName, key, createBody(data), metadata).withSSECustomerKey(SSE_C));

		var e = assertThrows(AmazonServiceException.class, () -> client.getObjectMetadata(bucketName, key));
		var statusCode = e.getStatusCode();
		assertEquals(400, statusCode);

		client.getObject(new GetObjectRequest(bucketName, key).withSSECustomerKey(SSE_C));
	}

	@Test
	@Tag("ERROR")
	//SSE-C 설정하여 업로드한 오브젝트를 SSE-C 설정없이 다운로드 실패 확인
	public void test_encryption_sse_c_present()
	{
		var bucketName = getNewBucket();
		var client = getClientHttps();
		var key = "obj";
		var size = 1000;
		var data = Utils.randomTextToLong(size);

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(size);

		var SSE_C = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
		.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
		.withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		client.putObject(new PutObjectRequest(bucketName, key, createBody(data), metadata).withSSECustomerKey(SSE_C));

		var e = assertThrows(AmazonServiceException.class, () -> client.getObject(bucketName, key));
		var statusCode = e.getStatusCode();
		assertEquals(400, statusCode);
	}

	@Test
	@Tag("ERROR")
	//SSE-C 설정하여 업로드한 오브젝트와 다른 SSE-C 설정으로 다운로드 실패 확인
	public void test_encryption_sse_c_other_key()
	{
		var bucketName = getNewBucket();
		var client = getClientHttps();
		var key = "obj";
		var size = 100;
		var data = Utils.randomTextToLong(size);

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(size);

		var SSE_C_A = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
				.withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		var SSE_C_B = new SSECustomerKey("6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=")
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
				.withMd5("arxBvwY2V4SiOne6yppVPQ==");

		client.putObject(new PutObjectRequest(bucketName, key, createBody(data), metadata).withSSECustomerKey(SSE_C_A));

		var e = assertThrows(AmazonServiceException.class, () -> client.getObject(new GetObjectRequest(bucketName, key).withSSECustomerKey(SSE_C_B)));
		var statusCode = e.getStatusCode();
		assertEquals(403, statusCode);
	}

	@Test
	@Tag("ERROR")
	//SSE-C 설정값중 key-md5값이 올바르지 않을 경우 업로드 실패 확인
	public void test_encryption_sse_c_invalid_md5()
	{
		var bucketName = getNewBucket();
		var client = getClientHttps();
		var key = "obj";
		var size = 100;
		var data = Utils.randomTextToLong(size);

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(size);

		var SSE_C = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
		.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
		.withMd5("AAAAAAAAAAAAAAAAAAAAAA==");

		var e = assertThrows(AmazonServiceException.class, () -> client.putObject(new PutObjectRequest(bucketName, key, createBody(data), metadata).withSSECustomerKey(SSE_C)));

		var statusCode = e.getStatusCode();
		assertEquals(400, statusCode);
	}


	@Test
	@Tag("ERROR")
	//SSE-C 설정값중 key-md5값을 누락했을 경우 업로드 성공 확인
	public void test_encryption_sse_c_no_md5()
	{
		var bucketName = getNewBucket();
		var client = getClientHttps();
		var key = "obj";
		var size = 100;
		var data = Utils.randomTextToLong(size);

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(size);

		var SSE_C = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
		.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

		client.putObject(new PutObjectRequest(bucketName, key, createBody(data), metadata).withSSECustomerKey(SSE_C));
		var response = client.getObject(new GetObjectRequest(bucketName, key).withSSECustomerKey(SSE_C));
		var body = getBody(response.getObjectContent());
		assertEquals(data, body);
		assertEquals(SSEAlgorithm.AES256.toString(), response.getObjectMetadata().getSSECustomerAlgorithm());
	}

	@Test
	@Tag("ERROR")
	//SSE-C 설정값중 key값을 누락했을 경우 업로드 실패 확인
	public void test_encryption_sse_c_no_key()
	{
		assertThrows(IllegalArgumentException.class, () -> new SSECustomerKey("").withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION));
	}

	@Test
	@Disabled("JAVA 에서는 algorithm값을 누락해도 기본값이 지정되어 있어 에러가 발생하지 않음")
	@Tag("ERROR")
	// @Tag("SSE-C 설정값중 algorithm값을 누락했을 경우 업로드 실패 확인
	public void test_encryption_key_no_sse_c()
	{
		var bucketName = getNewBucket();
		var client = getClientHttps();
		var key = "obj";
		var size = 100;
		var data = Utils.randomTextToLong(size);

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(size);

		var SSE_C = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
		.withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		var e = assertThrows(AmazonServiceException.class, () -> client.putObject(new PutObjectRequest(bucketName, key, createBody(data), metadata).withSSECustomerKey(SSE_C)));
		var statusCode = e.getStatusCode();
		assertEquals(400, statusCode);
	}

	@Test
	@Tag("Multipart")
	//멀티파트업로드를 SSE-C 설정하여 업로드 가능 확인
	public void test_encryption_sse_c_multipart_upload()
	{
		var bucketName = getNewBucket();
		var client = getClientHttps();
		var key = "multipart_enc";
		var size = 50 * MainData.MB;
		var contentType = "text/plain";
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("x-amz-meta-foo", "bar");
		metadata.setContentType(contentType);
		var SSE_C = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
		.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
		.withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		var uploadData = setupMultipartUpload(client, bucketName, key, size, 0, metadata, SSE_C);

		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));

		var headResponse = client.listObjectsV2(bucketName);
		var objectCount = headResponse.getKeyCount();
		assertEquals(1, objectCount);
		var bytesUsed = GetBytesUsed(headResponse);
		assertEquals(size, bytesUsed);

		var getResponse = client.getObject(new GetObjectRequest(bucketName, key).withSSECustomerKey(SSE_C));
		assertEquals(metadata.getUserMetadata(), getResponse.getObjectMetadata().getUserMetadata());
		assertEquals(contentType, getResponse.getObjectMetadata().getContentType());
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getObjectMetadata().getSSECustomerAlgorithm());

		var body = uploadData.getBody();
		CheckContentUsingRangeEnc(client, bucketName, key, body, MainData.MB, SSE_C);
		CheckContentUsingRangeEnc(client, bucketName, key, body, 10 * MainData.MB, SSE_C);
		checkContentUsingRandomRangeEnc(client, bucketName, key, body, size, 100, SSE_C);
	}


	@Test
	@Tag("Multipart")
	//SSE-C 설정하여 멀티파트 업로드한 오브젝트와 다른 SSE-C 설정으로 다운로드 실패 확인
	public void test_encryption_sse_c_multipart_bad_download()
	{
		var bucketName = getNewBucket();
		var client = getClientHttps();
		var key = "multipart_enc";
		var size = 50 * MainData.MB;
		var contentType = "text/plain";
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("x-amz-meta-foo", "bar");
		metadata.setContentType(contentType);

		var SetSSE_C = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
				.withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		var GetSSE_C = new SSECustomerKey("6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=")
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
				.withMd5("arxBvwY2V4SiOne6yppVPQ==");

		var uploadData = setupMultipartUpload(client, bucketName, key, size, 0, metadata, SetSSE_C);

		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, key, uploadData.uploadId, uploadData.parts));

		var headResponse = client.listObjectsV2(bucketName);
		var objectCount = headResponse.getKeyCount();
		assertEquals(1, objectCount);
		var bytesUsed = GetBytesUsed(headResponse);
		assertEquals(size, bytesUsed);

		var getResponse = client.getObject(new GetObjectRequest(bucketName, key).withSSECustomerKey(SetSSE_C));
		assertEquals(metadata.getUserMetadata(), getResponse.getObjectMetadata().getUserMetadata());
		assertEquals(contentType, getResponse.getObjectMetadata().getContentType());
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getObjectMetadata().getSSECustomerAlgorithm());

		var e = assertThrows(AmazonServiceException.class, ()-> client.getObject(new GetObjectRequest(bucketName, key).withSSECustomerKey(GetSSE_C)));
		var statusCode = e.getStatusCode();
		assertEquals(403, statusCode);
	}


	@Test
	@Tag("Post")
	//Post 방식으로 SSE-C 설정하여 오브젝트 업로드가 올바르게 동작하는지 확인
	public void test_encryption_sse_c_post_object_authenticated_request() throws MalformedURLException
	{
		assumeFalse(config.isAWS());
		var bucketName = getNewBucket();
		var client = getClientHttps();

		var contentType = "text/plain";
		var key = "foo.txt";
		var policyDocument = new JsonObject();
		policyDocument.addProperty("expiration", getTimeToAddMinutes(100));

		var conditions = new JsonArray();

		var bucket = new JsonObject();
		bucket.addProperty("bucket", bucketName);
		conditions.add(bucket);

		var starts1 = new JsonArray();
		starts1.add("starts-with");
		starts1.add("$key");
		starts1.add("foo");
		conditions.add(starts1);

		var acl = new JsonObject();
		acl.addProperty("acl", "private");
		conditions.add(acl);

		var starts2 = new JsonArray();
		starts2.add("starts-with");
		starts2.add("$Content-Type");
		starts2.add(contentType);
		conditions.add(starts2);


		var starts3 = new JsonArray();
		starts3.add("starts-with");
		starts3.add("$x-amz-server-side-encryption-customer-algorithm");
		starts3.add("AES256");
		conditions.add(starts3);

		var starts4 = new JsonArray();
		starts4.add("starts-with");
		starts4.add("$x-amz-server-side-encryption-customer-key");
		starts4.add("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=");
		conditions.add(starts4);

		var starts5 = new JsonArray();
		starts5.add("starts-with");
		starts5.add("$x-amz-server-side-encryption-customer-key-md5");
		starts5.add("DWygnHRtgiJ77HCm+1rvHw==");
		conditions.add(starts5);

		var contentLengthRange = new JsonArray();
		contentLengthRange.add("content-length-range");
		contentLengthRange.add(0);
		contentLengthRange.add(1024);
		conditions.add(contentLengthRange);

		policyDocument.add("conditions", conditions);

		var BytesJsonPolicyDocument = policyDocument.toString().getBytes();
		var encoder = Base64.getEncoder();
		var policy = encoder.encodeToString(BytesJsonPolicyDocument);

		var Signature = AWS2SignerBase.GetBase64EncodedSHA1Hash(policy, config.mainUser.secretKey);
		var FileData = new FormFile(key, contentType, "bar");
		var payload = new HashMap<String, String>();
		payload.put( "key", key );
		payload.put( "AWSAccessKeyId", config.mainUser.accessKey );
		payload.put( "acl", "private" );
		payload.put( "signature", Signature );
		payload.put( "policy", policy );
		payload.put( "Content-Type", contentType );
		payload.put( "x-amz-server-side-encryption-customer-algorithm", "AES256" );
		payload.put( "x-amz-server-side-encryption-customer-key", "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=" );
		payload.put( "x-amz-server-side-encryption-customer-key-md5", "DWygnHRtgiJ77HCm+1rvHw==" );

		var sendURL = getURL(bucketName);
		var result = NetUtils.postUpload(sendURL, payload, FileData);
		assertEquals(204, result.statusCode);

		var response = client.getObject(new GetObjectRequest(bucketName, key).withSSECustomerKey(new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")));
		var body = getBody(response.getObjectContent());
		assertEquals("bar", body);
		assertEquals(SSEAlgorithm.AES256.toString(), response.getObjectMetadata().getSSECustomerAlgorithm());
	}

	@Test
	@Tag("Get")
	//SSE-C설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인
	public void test_encryption_sse_c_get_object_many()
	{
		var bucketName = getNewBucket();
		var client = getClientHttps();
		var key = "obj";
		var size = 15 * 1024 * 1024;
		var data = Utils.randomTextToLong(size);

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(size);

		var SSE_C = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=").
					   withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION).
					   withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		client.putObject(new PutObjectRequest(bucketName, key, createBody(data), metadata).withSSECustomerKey(SSE_C));
		CheckContentEnc(bucketName, key, data, 50, SSE_C);
	}

	@Test
	@Tag("Get")
	//SSE-C설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
	public void test_encryption_sse_c_range_object_many()
	{
		var bucketName = getNewBucket();
		var client = getClientHttps();
		var key = "obj";
		var size = 15 * 1024 * 1024;
		var data = Utils.randomTextToLong(size);

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(size);

		var SSE_C = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=").
					   withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION).
					   withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		client.putObject(new PutObjectRequest(bucketName, key, createBody(data), metadata).withSSECustomerKey(SSE_C));

		var response = client.getObject(new GetObjectRequest(bucketName, key).withSSECustomerKey(SSE_C));
		var body = getBody(response.getObjectContent());
		assertTrue(data.equals(body), MainData.NOT_MATCHED);
		assertEquals(SSEAlgorithm.AES256.toString(), response.getObjectMetadata().getSSECustomerAlgorithm());

		checkContentUsingRandomRangeEnc(client, bucketName, key, data, size, 50, SSE_C);
	}

	@Test
	@Tag("Multipart")
	//SSE-C 설정하여 멀티파트로 업로드한 오브젝트를 mulitcopy 로 복사 가능한지 확인
	public void test_sse_c_encryption_multipart_copypart_upload()
	{
		var bucketName = getNewBucket();
		var client = getClientHttps();
		var sourceKey = "multipart_enc";
		var size = 50 * MainData.MB;
		var contentType = "text/plain";

		var metadata = new ObjectMetadata();
		metadata.setContentType("text/plain");
		metadata.setContentLength(size);

		var SSE_C = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=").
					   withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION).
					   withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		var uploadData = setupMultipartUpload(client, bucketName, sourceKey, size, 0, metadata, SSE_C);

		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, sourceKey, uploadData.uploadId, uploadData.parts));

		var headResponse = client.listObjectsV2(bucketName);
		var objectCount = headResponse.getKeyCount();
		assertEquals(1, objectCount);
		var bytesUsed = GetBytesUsed(headResponse);
		assertEquals(size, bytesUsed);

		var getResponse = client.getObject(new GetObjectRequest(bucketName, sourceKey).withSSECustomerKey(SSE_C));
		assertEquals(metadata.getUserMetadata(), getResponse.getObjectMetadata().getUserMetadata());
		assertEquals(contentType, getResponse.getObjectMetadata().getContentType());
		assertEquals(SSEAlgorithm.AES256.toString(), getResponse.getObjectMetadata().getSSECustomerAlgorithm());

		// 멀티파트 복사
		var targetKey = "multipart_enc_copy";
		uploadData = MultipartCopySSE_C(client, bucketName, sourceKey, bucketName, targetKey, size, metadata, SSE_C);
		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, targetKey, uploadData.uploadId, uploadData.parts));
		CheckCopyContentSSE_C(client, bucketName, sourceKey, bucketName, targetKey, SSE_C);
	}

	@Test
	@Tag("Multipart")
	//SSE-C 설정하여 Multipart와 Copypart를 모두 사용하여 오브젝트가 업로드 가능한지 확인
	public void test_sse_c_encryption_multipart_copy_many()
	{
		var bucketName = getNewBucket();
		var client = getClientHttps();
		var sourceKey = "multipart_enc";
		var size = 50 * MainData.MB;
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
		var targetKey1 = "mymultipart1";
		uploadData = multipartCopy(client, bucketName, sourceKey, bucketName, targetKey1, size, metadata);
		// 추가파츠 업로드
		uploadData = multipartUpload(client, bucketName, targetKey1, size, uploadData);
		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, targetKey1, uploadData.uploadId, uploadData.parts));

		// 업로드가 올바르게 되었는지 확인
		body.append(uploadData.body);
		checkContentUsingRange(bucketName, targetKey1, body.toString(), MainData.MB);

		// 멀티파트 카피
		var TargetKey2 = "mymultipart2";
		uploadData = multipartCopy(client, bucketName, targetKey1, bucketName, TargetKey2, size * 2, metadata);
		// 추가파츠 업로드
		uploadData = multipartUpload(client, bucketName, TargetKey2, size, uploadData);
		client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, TargetKey2, uploadData.uploadId, uploadData.parts));

		// 업로드가 올바르게 되었는지 확인
		body.append(uploadData.body);
		checkContentUsingRange(bucketName, TargetKey2, body.toString(), MainData.MB);
	}
}