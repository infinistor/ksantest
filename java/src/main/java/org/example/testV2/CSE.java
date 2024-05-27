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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.example.Data.AES256;
import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.CompletedPart;

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
	// [AES256] 1Byte 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인
	public void testCseEncryptedTransfer1b() {
		testEncryptionCSEWrite(1);
	}

	@Test
	@Tag("PutGet")
	// [AES256] 1KB 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인
	public void testCseEncryptedTransfer1kb() {
		testEncryptionCSEWrite(1024);
	}

	@Test
	@Tag("PutGet")
	// [AES256] 1MB 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인
	public void testCseEncryptedTransfer1MB() {
		testEncryptionCSEWrite(1024 * 1024);
	}

	@Test
	@Tag("PutGet")
	// [AES256] 13Byte 오브젝트를 암호화 하여 업로드한뒤, 다운로드하여 복호화 했을 경우 일치하는지 확인
	public void testCseEncryptedTransfer13b() {
		testEncryptionCSEWrite(13);
	}

	@Test
	@Tag("Metadata")
	// [AES256] 암호화하고 메타데이터에 키값을 추가하여 업로드한 오브젝트가 올바르게 반영되었는지 확인
	public void testCseEncryptionMethodHead() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "obj";
		var size = 1000;
		var contentType = "text/plain";
		var data = Utils.randomTextToLong(size);

		// AES
		var aesKey = Utils.randomTextToLong(32);
		try {
			var encoding = AES256.encrypt(data, aesKey);
			var metadata = new HashMap<String, String>();
			metadata.put("x-amz-meta-key", aesKey);

			client.putObject(p -> p.bucket(bucketName).key(key).contentLength((long) encoding.length())
					.contentType(contentType), RequestBody.fromString(encoding));

			var getMetadata = client.headObject(h -> h.bucket(bucketName).key(key));
			assertEquals(metadata, getMetadata.metadata());
		} catch (Exception e) {
			fail(e);
		}
	}

	@Test
	@Tag("ERROR")
	// [AES256] 암호화 하여 업로드한 오브젝트를 다운로드하여 비교할경우 불일치
	public void testCseEncryptionNonDecryption() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "obj";
		var size = 1000;
		var contentType = "text/plain";
		var data = Utils.randomTextToLong(size);

		// AES
		var aesKey = Utils.randomTextToLong(32);

		try {
			var encoding = AES256.encrypt(data, aesKey);
			var metadata = new HashMap<String, String>();
			metadata.put("x-amz-meta-key", aesKey);

			client.putObject(p -> p.bucket(bucketName).key(key).contentLength((long) encoding.length())
					.contentType(contentType), RequestBody.fromString(encoding));

			var response = client.getObject(g -> g.bucket(bucketName).key(key));
			var body = getBody(response);
			assertNotEquals(data, body);
		} catch (Exception e) {
			fail(e);
		}
	}

	@Test
	@Tag("ERROR")
	// [AES256] 암호화 없이 업로드한 오브젝트를 다운로드하여 복호화할 경우 실패 확인
	public void testCseNonEncryptionDecryption() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "obj";
		var size = 1000;
		var contentType = "text/plain";
		var data = Utils.randomTextToLong(size);

		// AES
		var aesKey = Utils.randomTextToLong(32);

		var metadata = new HashMap<String, String>();
		metadata.put("x-amz-meta-key", aesKey);

		client.putObject(p -> p.bucket(bucketName).key(key).contentType(contentType),
				RequestBody.fromString(data));

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		var encodingBody = getBody(response);
		assertThrows(Exception.class, () -> AES256.decrypt(encodingBody, aesKey));
	}

	@Test
	@Tag("RangeRead")
	// [AES256] 암호화 하여 업로드한 오브젝트에 대해 범위를 지정하여 읽기 성공
	public void testCseEncryptionRangeRead() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "obj";
		var contentType = "text/plain";

		// AES
		var aesKey = Utils.randomTextToLong(32);

		try {
			var data = Utils.randomTextToLong(1024 * 1024);
			var encoding = AES256.encrypt(data, aesKey);
			var metadata = new HashMap<String, String>();
			metadata.put("x-amz-meta-key", aesKey);

			client.putObject(p -> p.bucket(bucketName).key(key).contentLength((long) encoding.length())
					.contentType(contentType), RequestBody.fromString(encoding));

			var r = new Random(System.currentTimeMillis());
			var startPoint = r.nextInt(1024 * 1024 - 1001);
			var endPoint = startPoint + 999;
			var response = client.getObject(
					g -> g.bucket(bucketName).key(key).range("bytes=" + startPoint + "-" + endPoint));
			var encodingBody = getBody(response);
			assertTrue(encoding.substring(startPoint, startPoint + 1000).equals(encodingBody),
					MainData.NOT_MATCHED);
		} catch (Exception e) {
			fail(e);
		}
	}

	@Test
	@Tag("Multipart")
	// [AES256] 암호화된 오브젝트 멀티파트 업로드 / 다운로드 성공 확인
	public void testCseEncryptionMultipartUpload() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "multipartEnc";
		var size = 50 * MainData.MB;
		var contentType = "text/plain";
		var data = Utils.randomTextToLong(size);

		// AES
		var aesKey = Utils.randomTextToLong(32);

		try {
			var encoding = AES256.encrypt(data, aesKey);
			var metadata = new HashMap<String, String>();
			metadata.put("x-amz-meta-key", aesKey);

			var initMultiPartResponse = client
					.createMultipartUpload(
							c -> c.bucket(bucketName).key(key).contentType(contentType).metadata(metadata));
			var uploadId = initMultiPartResponse.uploadId();

			var parts = cutStringData(encoding, 5 * MainData.MB);
			var partETags = new ArrayList<CompletedPart>();
			for (var part : parts) {
				int partNumber = partETags.size() + 1;
				var partResPonse = client.uploadPart(
						u -> u.bucket(bucketName).key(key).uploadId(uploadId).partNumber(partNumber),
						RequestBody.fromString(part));
				partETags.add(CompletedPart.builder().partNumber(partNumber).eTag(partResPonse.eTag()).build());
			}

			client.completeMultipartUpload(c -> c.bucket(bucketName).key(key).uploadId(uploadId)
					.multipartUpload(p->p.parts(partETags)));

			var headResponse = client.listObjectsV2(l -> l.bucket(bucketName));
			assertEquals(1, headResponse.keyCount());
			assertEquals(encoding.length(), getBytesUsed(headResponse));

			var getResponse = client.headObject(h -> h.bucket(bucketName).key(key));
			assertEquals(metadata, getResponse.metadata());
			assertEquals(contentType, getResponse.contentType());

			checkContentUsingRange(bucketName, key, encoding, MainData.MB);
			checkContentUsingRange(bucketName, key, encoding, 10L * MainData.MB);
			checkContentUsingRandomRange(bucketName, key, encoding, 100);
		} catch (Exception e) {
			fail(e);
		}
	}

	@Test
	@Tag("Get")
	// CSE설정한 오브젝트를 여러번 반복하여 다운로드 성공 확인
	public void testCseGetObjectMany() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";
		var contentType = "text/plain";
		// AES
		var aesKey = Utils.randomTextToLong(32);
		var data = Utils.randomTextToLong(15 * MainData.MB);

		try {
			var encoding = AES256.encrypt(data, aesKey);
			var metadata = new HashMap<String, String>();
			metadata.put("AESkey", aesKey);

			client.putObject(p -> p.bucket(bucketName).key(key).contentLength((long) encoding.length())
					.contentType(contentType), RequestBody.fromString(encoding));

			var response = client.getObject(g -> g.bucket(bucketName).key(key));
			var encodingBody = getBody(response);
			var body = AES256.decrypt(encodingBody, aesKey);
			assertTrue(data.equals(body), MainData.NOT_MATCHED);
			checkContent(bucketName, key, encoding, 50);

		} catch (Exception e) {
			fail(e);
		}
	}

	@Test
	@Tag("Get")
	// CSE설정한 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
	public void testCseRangeObjectMany() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";
		var contentType = "text/plain";

		// AES
		var aesKey = Utils.randomTextToLong(32);
		var fileSize = 15 * 1024 * 1024;
		var data = Utils.randomTextToLong(fileSize);

		try {
			var encoding = AES256.encrypt(data, aesKey);
			var metadata = new HashMap<String, String>();
			metadata.put("AESkey", aesKey);

			client.putObject(p -> p.bucket(bucketName).key(key).contentLength((long) encoding.length())
					.contentType(contentType), RequestBody.fromString(encoding));

			var response = client.getObject(g -> g.bucket(bucketName).key(key));
			var encodingBody = getBody(response);
			var body = AES256.decrypt(encodingBody, aesKey);
			assertTrue(data.equals(body), MainData.NOT_MATCHED);

			checkContentUsingRandomRange(bucketName, key, encoding, 50);

		} catch (Exception e) {
			fail(e);
		}
	}
}
