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
package org.example.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Random;

import org.example.Data.AES256;
import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;

public class CSE extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("CSE Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("CSE End");
	}

	@Test
	@Tag("PutGet")
	// @Tag("[AES256] 1Byte 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인
	public void test_cse_encrypted_transfer_1b() {
		testEncryptionCSEWrite(1);
	}

	@Test
	@Tag("PutGet")
	// @Tag("[AES256] 1KB 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인
	public void test_cse_encrypted_transfer_1kb() {
		testEncryptionCSEWrite(1024);
	}

	@Test
	@Tag("PutGet")
	// @Tag("[AES256] 1MB 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인
	public void test_cse_encrypted_transfer_1MB() {
		testEncryptionCSEWrite(1024 * 1024);
	}

	@Test
	@Tag("PutGet")
	// @Tag("[AES256] 13Byte 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인
	public void test_cse_encrypted_transfer_13b() {
		testEncryptionCSEWrite(13);
	}

	@Test
	@Tag("Metadata")
	// @Tag("[AES256] 암호화하고 메타데이터에 키값을 추가하여 업로드한 오브젝트가 올바르게 반영되었는지 확인 
	public void test_cse_encryption_method_head() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "testobj";
		var size = 1000;
		var data = Utils.randomTextToLong(size);

		// AES
		var aesKey = Utils.randomTextToLong(32);
		try {
			var encoding = AES256.encrypt(data, aesKey);
			var metadata = new ObjectMetadata();
			metadata.addUserMetadata("x-amz-meta-key", aesKey);
			metadata.setContentType("text/plain");
			metadata.setContentLength(encoding.length());

			client.putObject(bucketName, key, createBody(encoding), metadata);

			var getMetadata = client.getObjectMetadata(bucketName, key);
			assertEquals(metadata.getUserMetadata(), getMetadata.getUserMetadata());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	@Tag("ERROR")
	// @Tag("[AES256] 암호화 하여 업로드한 오브젝트를 다운로드하여 비교할경우 불일치
	public void test_cse_encryption_non_decryption() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "testobj";
		var size = 1000;
		var data = Utils.randomTextToLong(size);

		// AES
		var aesKey = Utils.randomTextToLong(32);

		try {
			var encoding = AES256.encrypt(data, aesKey);
			var metadata = new ObjectMetadata();
			metadata.addUserMetadata("x-amz-meta-key", aesKey);
			metadata.setContentType("text/plain");
			metadata.setContentLength(encoding.length());

			client.putObject(bucketName, key, createBody(encoding), metadata);

			var response = client.getObject(bucketName, key);
			var body = getBody(response.getObjectContent());
			assertNotEquals(data, body);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	@Tag("ERROR")
	// @Tag("[AES256] 암호화 없이 업로드한 오브젝트를 다운로드하여 복호화할 경우 실패 확인
	public void test_cse_non_encryption_decryption() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "testobj";
		var size = 1000;
		var data = Utils.randomTextToLong(size);

		// AES
		var aesKey = Utils.randomTextToLong(32);

		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("x-amz-meta-key", aesKey);
		metadata.setContentType("text/plain");
		metadata.setContentLength(size);
		client.putObject(bucketName, key, createBody(data), metadata);

		var response = client.getObject(bucketName, key);
		var encodingBody = getBody(response.getObjectContent());
		assertThrows(Exception.class, () -> AES256.decrypt(encodingBody, aesKey));
	}

	@Test
	@Tag("RangeRead")
	// @Tag("[AES256] 암호화 하여 업로드한 오브젝트에 대해 범위를 지정하여 읽기 성공
	public void test_cse_encryption_range_read() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "testobj";

		// AES
		var aesKey = Utils.randomTextToLong(32);

		try {
			var data = Utils.randomTextToLong(1024 * 1024);
			var encoding = AES256.encrypt(data, aesKey);
			var metadata = new ObjectMetadata();
			metadata.addUserMetadata("x-amz-meta-key", aesKey);
			metadata.setContentType("text/plain");
			metadata.setContentLength(encoding.length());
			client.putObject(bucketName, key, createBody(encoding), metadata);

			var r = new Random();
			var startPoint = r.nextInt(1024 * 1024 - 1001);
			var endPoint = startPoint + 999;
			var response = client.getObject(new GetObjectRequest(bucketName, key).withRange(startPoint, endPoint));
			var encodingBody = getBody(response.getObjectContent());
			assertTrue(encoding.substring(startPoint, startPoint + 1000).equals(encodingBody),
					MainData.NOT_MATCHED);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	@Tag("Multipart")
	// @Tag("[AES256] 암호화된 오브젝트 멀티파트 업로드 / 다운로드 성공 확인
	public void test_cse_encryption_multipart_upload() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "multipart_enc";
		var size = 50 * MainData.MB;
		var contentType = "text/plain";
		var data = Utils.randomTextToLong(size);

		// AES
		var aesKey = Utils.randomTextToLong(32);

		try {
			var encoding = AES256.encrypt(data, aesKey);
			var metadata = new ObjectMetadata();
			metadata.addUserMetadata("x-amz-meta-key", aesKey);
			metadata.setContentType(contentType);

			var initMultiPartResponse = client
					.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key, metadata));
			var uploadID = initMultiPartResponse.getUploadId();

			var parts = CutStringData(encoding, 5 * MainData.MB);
			var partETags = new ArrayList<PartETag>();
			int partNumber = 1;
			for (var part : parts) {
				var partResPonse = client.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(key)
						.withUploadId(uploadID).withPartNumber(partNumber++).withInputStream(createBody(part))
						.withPartSize(part.length()));
				partETags.add(partResPonse.getPartETag());
			}

			client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, key, uploadID, partETags));

			var headResponse = client.listObjectsV2(bucketName);
			var objectCount = headResponse.getKeyCount();
			assertEquals(1, objectCount);
			assertEquals(encoding.length(), GetBytesUsed(headResponse));

			var getResponse = client.getObjectMetadata(bucketName, key);
			assertEquals(metadata.getUserMetadata(), getResponse.getUserMetadata());
			assertEquals(contentType, getResponse.getContentType());

			checkContentUsingRange(bucketName, key, encoding, MainData.MB);
			checkContentUsingRange(bucketName, key, encoding, 10 * MainData.MB);
			checkContentUsingRandomRange(bucketName, key, encoding, 100);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	@Tag("Get")
	// @Tag("CSE설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인
	public void test_cse_get_object_many() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";
		// AES
		var aesKey = Utils.randomTextToLong(32);
		var data = Utils.randomTextToLong(15 * MainData.MB);

		try {
			var encoding = AES256.encrypt(data, aesKey);
			var metadata = new ObjectMetadata();
			metadata.addUserMetadata("AESkey", aesKey);
			metadata.setContentType("text/plain");
			metadata.setContentLength(encoding.length());

			client.putObject(bucketName, key, createBody(encoding), metadata);

			var response = client.getObject(bucketName, key);
			var encodingBody = getBody(response.getObjectContent());
			var body = AES256.decrypt(encodingBody, aesKey);
			assertTrue(data.equals(body), MainData.NOT_MATCHED);
			checkContent(bucketName, key, encoding, 50);

		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	@Tag("Get")
	// @Tag("CSE설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
	public void test_cse_range_object_many() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		// AES
		var aesKey = Utils.randomTextToLong(32);
		var fileSize = 15 * 1024 * 1024;
		var data = Utils.randomTextToLong(fileSize);

		try {
			var encoding = AES256.encrypt(data, aesKey);
			var metadata = new ObjectMetadata();
			metadata.addUserMetadata("AESkey", aesKey);
			metadata.setContentType("text/plain");
			metadata.setContentLength(encoding.length());

			client.putObject(bucketName, key, createBody(encoding), metadata);

			var response = client.getObject(bucketName, key);
			var encodingBody = getBody(response.getObjectContent());
			var body = AES256.decrypt(encodingBody, aesKey);
			assertTrue(data.equals(body), MainData.NOT_MATCHED);

			checkContentUsingRandomRange(bucketName, key, encoding, 50);

		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
}
