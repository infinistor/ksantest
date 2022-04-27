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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Random;

import org.example.s3tests.AES256;
import org.example.s3tests.MainData;
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
	static public void BeforeAll() {
		System.out.println("CSE Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll() {
		System.out.println("CSE End");
	}

	@Test
	@Tag("PutGet")
	// @Tag("[AES256] 1Byte 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인
	public void test_cse_encrypted_transfer_1b() {
		TestEncryptionCSEWrite(1);
	}

	@Test
	@Tag("PutGet")
	// @Tag("[AES256] 1KB 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인
	public void test_cse_encrypted_transfer_1kb() {
		TestEncryptionCSEWrite(1024);
	}

	@Test
	@Tag("PutGet")
	// @Tag("[AES256] 1MB 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인
	public void test_cse_encrypted_transfer_1MB() {
		TestEncryptionCSEWrite(1024 * 1024);
	}

	@Test
	@Tag("PutGet")
	// @Tag("[AES256] 13Byte 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인
	public void test_cse_encrypted_transfer_13b() {
		TestEncryptionCSEWrite(13);
	}

	@Test
	@Tag("Metadata")
	// @Tag("[AES256] 암호화하고 메타데이터에 키값을 추가하여 업로드한 오브젝트가 올바르게 반영되었는지 확인 
	public void test_cse_encryption_method_head() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "testobj";
		var Size = 1000;
		var Data = RandomTextToLong(Size);

		// AES
		var AESKey = RandomTextToLong(32);
		try {
			var EncodingData = AES256.encryptAES256(Data, AESKey);
			var MetadataList = new ObjectMetadata();
			MetadataList.addUserMetadata("x-amz-meta-key", AESKey);
			MetadataList.setContentType("text/plain");
			MetadataList.setContentLength(EncodingData.length());

			Client.putObject(BucketName, Key, CreateBody(EncodingData), MetadataList);

			var ResMetaData = Client.getObjectMetadata(BucketName, Key);
			assertEquals(MetadataList.getUserMetadata(), ResMetaData.getUserMetadata());
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	@Tag("ERROR")
	// @Tag("[AES256] 암호화 하여 업로드한 오브젝트를 다운로드하여 비교할경우 불일치
	public void test_cse_encryption_non_decryption() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "testobj";
		var Size = 1000;
		var Data = RandomTextToLong(Size);

		// AES
		var AESKey = RandomTextToLong(32);

		try {
			var EncodingData = AES256.encryptAES256(Data, AESKey);
			var MetadataList = new ObjectMetadata();
			MetadataList.addUserMetadata("x-amz-meta-key", AESKey);
			MetadataList.setContentType("text/plain");
			MetadataList.setContentLength(EncodingData.length());

			Client.putObject(BucketName, Key, CreateBody(EncodingData), MetadataList);

			var Response = Client.getObject(BucketName, Key);
			var Body = GetBody(Response.getObjectContent());
			assertNotEquals(Data, Body);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	@Tag("ERROR")
	// @Tag("[AES256] 암호화 없이 업로드한 오브젝트를 다운로드하여 복호화할 경우 실패 확인
	public void test_cse_non_encryption_decryption() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "testobj";
		var Size = 1000;
		var Data = RandomTextToLong(Size);

		// AES
		var AESKey = RandomTextToLong(32);

		var MetadataList = new ObjectMetadata();
		MetadataList.addUserMetadata("x-amz-meta-key", AESKey);
		MetadataList.setContentType("text/plain");
		MetadataList.setContentLength(Size);
		Client.putObject(BucketName, Key, CreateBody(Data), MetadataList);

		var Response = Client.getObject(BucketName, Key);
		var EncodingBody = GetBody(Response.getObjectContent());
		assertThrows(Exception.class, () -> AES256.decryptAES256(EncodingBody, AESKey));
	}

	@Test
	@Tag("RangeRead")
	// @Tag("[AES256] 암호화 하여 업로드한 오브젝트에 대해 범위를 지정하여 읽기 성공
	public void test_cse_encryption_range_read() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "testobj";

		// AES
		var AESKey = RandomTextToLong(32);

		try {
			var Data = RandomTextToLong(1024 * 1024);
			var EncodingData = AES256.encryptAES256(Data, AESKey);
			var MetadataList = new ObjectMetadata();
			MetadataList.addUserMetadata("x-amz-meta-key", AESKey);
			MetadataList.setContentType("text/plain");
			MetadataList.setContentLength(EncodingData.length());
			Client.putObject(BucketName, Key, CreateBody(EncodingData), MetadataList);

			var r = new Random();
			var StartPoint = r.nextInt(1024 * 1024 - 1001);
			var Response = Client
					.getObject(new GetObjectRequest(BucketName, Key).withRange(StartPoint, StartPoint + 1000 - 1));
			var EncodingBody = GetBody(Response.getObjectContent());
			assertTrue(EncodingData.substring(StartPoint, StartPoint + 1000).equals(EncodingBody),
					"Source does not match target");
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	@Tag("Multipart")
	// @Tag("[AES256] 암호화된 오브젝트 멀티파트 업로드 / 다운로드 성공 확인
	public void test_cse_encryption_multipart_upload() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "multipart_enc";
		var Size = 50 * MainData.MB;
		var ContentType = "text/plain";
		var Data = RandomTextToLong(Size);

		// AES
		var AESKey = RandomTextToLong(32);

		try {
			var EncodingData = AES256.encryptAES256(Data, AESKey);
			var MetadataList = new ObjectMetadata();
			MetadataList.addUserMetadata("x-amz-meta-key", AESKey);
			MetadataList.setContentType(ContentType);

			var InitMultiPartResponse = Client
					.initiateMultipartUpload(new InitiateMultipartUploadRequest(BucketName, Key, MetadataList));
			var UploadID = InitMultiPartResponse.getUploadId();

			var Parts = CutStringData(EncodingData, 5 * MainData.MB);
			var PartETag = new ArrayList<PartETag>();
			int PartNumber = 1;
			for (var Part : Parts) {
				var PartResPonse = Client.uploadPart(new UploadPartRequest().withBucketName(BucketName).withKey(Key)
						.withUploadId(UploadID).withPartNumber(PartNumber++).withInputStream(CreateBody(Part))
						.withPartSize(Part.length()));
				PartETag.add(PartResPonse.getPartETag());
			}

			Client.completeMultipartUpload(new CompleteMultipartUploadRequest(BucketName, Key, UploadID, PartETag));

			var HeadResponse = Client.listObjectsV2(BucketName);
			var ObjectCount = HeadResponse.getKeyCount();
			assertEquals(1, ObjectCount);
			assertEquals(EncodingData.length(), GetBytesUsed(HeadResponse));

			var GetResponse = Client.getObjectMetadata(BucketName, Key);
			assertEquals(MetadataList.getUserMetadata(), GetResponse.getUserMetadata());
			assertEquals(ContentType, GetResponse.getContentType());

			CheckContentUsingRange(BucketName, Key, EncodingData, MainData.MB);
			CheckContentUsingRange(BucketName, Key, EncodingData, 10 * MainData.MB);
			CheckContentUsingRandomRange(BucketName, Key, EncodingData, 100);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	@Tag("Get")
	// @Tag("CSE설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인
	public void test_cse_get_object_many() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "foo";
		// AES
		var AESKey = RandomTextToLong(32);
		var Data = RandomTextToLong(15 * MainData.MB);

		try {
			var EncodingData = AES256.encryptAES256(Data, AESKey);
			var MetadataList = new ObjectMetadata();
			MetadataList.addUserMetadata("AESkey", AESKey);
			MetadataList.setContentType("text/plain");
			MetadataList.setContentLength(EncodingData.length());

			Client.putObject(BucketName, Key, CreateBody(EncodingData), MetadataList);

			var Response = Client.getObject(BucketName, Key);
			var EncodingBody = GetBody(Response.getObjectContent());
			var Body = AES256.decryptAES256(EncodingBody, AESKey);
			assertTrue(Data.equals(Body), "Source does not match target");
			CheckContent(BucketName, Key, EncodingData, 50);

		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	@Tag("Get")
	// @Tag("CSE설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
	public void test_cse_range_object_many() {
		var BucketName = GetNewBucket();
		var Client = GetClient();
		var Key = "foo";

		// AES
		var AESKey = RandomTextToLong(32);
		var FileSize = 15 * 1024 * 1024;
		var Data = RandomTextToLong(FileSize);

		try {
			var EncodingData = AES256.encryptAES256(Data, AESKey);
			var MetadataList = new ObjectMetadata();
			MetadataList.addUserMetadata("AESkey", AESKey);
			MetadataList.setContentType("text/plain");
			MetadataList.setContentLength(EncodingData.length());

			Client.putObject(BucketName, Key, CreateBody(EncodingData), MetadataList);

			var Response = Client.getObject(BucketName, Key);
			var EncodingBody = GetBody(Response.getObjectContent());
			var Body = AES256.decryptAES256(EncodingBody, AESKey);
			assertTrue(Data.equals(Body), "Source does not match target");

			CheckContentUsingRandomRange(BucketName, Key, EncodingData, 50);

		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
}
