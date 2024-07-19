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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.MetadataDirective;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSECustomerKey;

@Execution(ExecutionMode.CONCURRENT)
public class CopyObject extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("CopyObject Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("CopyObject End");
	}

	@Test
	@Tag("Check")
	// 오브젝트의 크기가 0일때 복사가 가능한지 확인
	public void testObjectCopyZeroSize() {
		var key = "foo123bar";
		var newKey = "bar321foo";
		var client = getClient();
		var bucketName = createObjects(client, List.of(key));

		client.putObject(bucketName, key, "");

		client.copyObject(bucketName, key, bucketName, newKey);

		var response = client.getObject(bucketName, newKey);
		assertEquals(0, response.getObjectMetadata().getContentLength());
	}

	@Test
	@Tag("Check")
	// 동일한 버킷에서 오브젝트 복사가 가능한지 확인
	public void testObjectCopySameBucket() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo123bar";
		var newKey = "bar321foo";

		client.putObject(bucketName, key, "foo");

		client.copyObject(bucketName, key, bucketName, newKey);

		var response = client.getObject(bucketName, newKey);
		var body = getBody(response.getObjectContent());
		assertEquals("foo", body);
	}

	@Test
	@Tag("ContentType")
	// ContentType을 설정한 오브젝트를 복사할 경우 복사된 오브젝트도 ContentType값이 일치하는지 확인
	public void testObjectCopyVerifyContentType() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo123bar";
		var newKey = "bar321foo";
		var contentType = "text/bla";
		var metaData = new ObjectMetadata();
		metaData.setContentType(contentType);
		metaData.setContentLength(3);

		client.putObject(new PutObjectRequest(bucketName, key, createBody("foo"), metaData));
		client.copyObject(bucketName, key, bucketName, newKey);

		var response = client.getObject(bucketName, newKey);
		var body = getBody(response.getObjectContent());
		assertEquals("foo", body);
		assertEquals(contentType, response.getObjectMetadata().getContentType());
	}

	@Test
	@Tag("Overwrite")
	// 복사할 오브젝트와 복사될 오브젝트의 경로가 같을 경우 에러 확인
	public void testObjectCopyToItself() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo123bar";

		client.putObject(bucketName, key, "");

		var e = assertThrows(AmazonServiceException.class, () -> client.copyObject(bucketName, key, bucketName, key));
		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();

		assertEquals(400, statusCode);
		assertEquals(MainData.INVALID_REQUEST, errorCode);
	}

	@Test
	@Tag("Overwrite")
	// 복사할 오브젝트와 복사될 오브젝트의 경로가 같지만 메타데이터를 덮어쓰기 모드로 추가하면 해당 오브젝트의 메타데이터가 업데이트되는지 확인
	public void testObjectCopyToItselfWithMetadata() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo123bar";

		client.putObject(bucketName, key, "foo");

		var metaData = new ObjectMetadata();
		metaData.addUserMetadata("foo", "bar");

		client.copyObject(new CopyObjectRequest(bucketName, key, bucketName, key).withNewObjectMetadata(metaData)
				.withMetadataDirective(MetadataDirective.REPLACE));
		var response = client.getObject(bucketName, key);

		assertEquals(metaData.getUserMetadata(), response.getObjectMetadata().getUserMetadata());
	}

	@Test
	@Tag("Check")
	// 다른 버킷으로 오브젝트 복사가 가능한지 확인
	public void testObjectCopyDiffBucket() {
		var client = getClient();
		var bucketName1 = createBucket(client);
		var bucketName2 = createBucket(client);
		var key1 = "foo123bar";
		var key2 = "bar321foo";

		client.putObject(bucketName1, key1, "foo");

		client.copyObject(bucketName1, key1, bucketName2, key2);

		var response = client.getObject(bucketName2, key2);
		var body = getBody(response.getObjectContent());
		assertEquals("foo", body);
	}

	@Test
	@Tag("Check")
	// [bucket1:created main user, object:created main user / bucket2:created sub
	// user] 메인유저가 만든 버킷, 오브젝트를 서브유저가 만든 버킷으로 오브젝트 복사가 불가능한지 확인
	public void testObjectCopyNotOwnedBucket() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName1 = getNewBucketName();
		var bucketName2 = getNewBucketName();

		client.createBucket(bucketName1);
		altClient.createBucket(bucketName2);

		var key1 = "foo123bar";
		var key2 = "bar321foo";

		client.putObject(bucketName1, key1, "foo");

		var e = assertThrows(AmazonServiceException.class,
				() -> altClient.copyObject(bucketName1, key1, bucketName2, key2));
		var statusCode = e.getStatusCode();

		assertEquals(403, statusCode);
		altClient.deleteBucket(bucketName2);
		deleteBucketList(bucketName2);
	}

	@Test
	@Tag("Check")
	// [bucketAcl = main:full control,sub : full control | objectAcl = default]
	// 서브유저가 접근권한이 있는 버킷에 들어있는 접근권한이 있는 오브젝트를 복사가 가능한지 확인
	public void testObjectCopyNotOwnedObjectBucket() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucketCannedACL(client);

		var key1 = "foo123bar";
		var key2 = "bar321foo";

		client.putObject(bucketName, key1, "foo");

		var altUserId = config.altUser.id;
		var myGrant = new Grant(new CanonicalGrantee(altUserId), Permission.FullControl);
		var grants = addObjectUserGrant(bucketName, key1, myGrant);
		client.setObjectAcl(bucketName, key1, grants);

		grants = addBucketUserGrant(bucketName, myGrant);
		client.setBucketAcl(bucketName, grants);

		altClient.getObject(bucketName, key1);

		altClient.copyObject(bucketName, key1, bucketName, key2);
	}

	@Test
	@Tag("Overwrite")
	// 권한정보를 포함하여 복사할때 올바르게 적용되는지 확인 메타데이터를 포함하여 복사할때 올바르게 적용되는지 확인
	public void testObjectCopyCannedAcl() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucketCannedACL(client);
		var key1 = "foo123bar";
		var key2 = "bar321foo";

		client.putObject(bucketName, key1, "foo");

		client.copyObject(new CopyObjectRequest(bucketName, key1, bucketName, key2)
				.withCannedAccessControlList(CannedAccessControlList.PublicRead));
		altClient.getObject(bucketName, key2);

		var metaData = new ObjectMetadata();
		metaData.addUserMetadata("x-amz-meta-abc", "def");

		client.copyObject(new CopyObjectRequest(bucketName, key2, bucketName, key1)
				.withCannedAccessControlList(CannedAccessControlList.PublicRead).withNewObjectMetadata(metaData)
				.withMetadataDirective(MetadataDirective.REPLACE));
		altClient.getObject(bucketName, key1);
	}

	@Test
	@Tag("Check")
	// 크고 작은 용량의 오브젝트가 복사되는지 확인
	public void testObjectCopyRetainingMetadata() {
		var client = getClient();
		for (var size : new int[] { 3, 1024 * 1024 }) {
			var bucketName = createBucket(client);
			var contentType = "audio/ogg";

			var key1 = "foo123bar";
			var key2 = "bar321foo";

			var metaData = new ObjectMetadata();
			metaData.addUserMetadata("x-amz-meta-key1", "value1");
			metaData.addUserMetadata("x-amz-meta-key2", "value2");
			metaData.setContentType(contentType);
			metaData.setContentLength(size);

			client.putObject(
					new PutObjectRequest(bucketName, key1, createBody(Utils.randomTextToLong(size)), metaData));
			client.copyObject(bucketName, key1, bucketName, key2);

			var response = client.getObject(bucketName, key2);
			assertEquals(contentType, response.getObjectMetadata().getContentType());
			assertEquals(metaData.getUserMetadata(), response.getObjectMetadata().getUserMetadata());
			assertEquals(size, response.getObjectMetadata().getContentLength());
		}
	}

	@Test
	@Tag("Check")
	// 크고 작은 용량의 오브젝트및 메타데이터가 복사되는지 확인
	public void testObjectCopyReplacingMetadata() {
		var client = getClient();
		for (var size : new int[] { 3, 1024 * 1024 }) {
			var bucketName = createBucket(client);
			var contentType = "audio/ogg";

			var key1 = "foo123bar";
			var key2 = "bar321foo";

			var metadata = new ObjectMetadata();
			metadata.addUserMetadata("x-amz-meta-key1", "value1");
			metadata.addUserMetadata("x-amz-meta-key2", "value2");
			metadata.setContentType(contentType);
			metadata.setContentLength(size);

			client.putObject(
					new PutObjectRequest(bucketName, key1, createBody(Utils.randomTextToLong(size)), metadata));

			var metadata2 = new ObjectMetadata();
			metadata2.addUserMetadata("x-amz-meta-key3", "value3");
			metadata2.addUserMetadata("x-amz-meta-key4", "value4");
			metadata2.setContentType(contentType);

			client.copyObject(new CopyObjectRequest(bucketName, key1, bucketName, key2).withNewObjectMetadata(metadata2)
					.withMetadataDirective(MetadataDirective.REPLACE));

			var response = client.getObject(bucketName, key2);
			assertEquals(contentType, response.getObjectMetadata().getContentType());
			assertEquals(metadata2.getUserMetadata(), response.getObjectMetadata().getUserMetadata());
			assertEquals(size, response.getObjectMetadata().getContentLength());
		}
	}

	@Test
	@Tag("ERROR")
	// 존재하지 않는 버킷에서 존재하지 않는 오브젝트 복사 실패 확인
	public void testObjectCopyBucketNotFound() {
		var client = getClient();
		var bucketName = createBucket(client);

		var e = assertThrows(AmazonServiceException.class,
				() -> client.copyObject(bucketName + "-fake", "foo123bar", bucketName, "bar321foo"));
		var statusCode = e.getStatusCode();
		assertEquals(404, statusCode);
	}

	@Test
	@Tag("ERROR")
	// 존재하지않는 오브젝트 복사 실패 확인
	public void testObjectCopyKeyNotFound() {
		var client = getClient();
		var bucketName = createBucket(client);

		var e = assertThrows(AmazonServiceException.class,
				() -> client.copyObject(bucketName, "foo123bar", bucketName, "bar321foo"));
		var statusCode = e.getStatusCode();
		assertEquals(404, statusCode);
	}

	@Test
	@Tag("Version")
	// 버저닝된 오브젝트 복사 확인
	public void testObjectCopyVersionedBucket() {
		var client = getClient();
		var bucketName = createBucket(client);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		var size = 1 * 5;
		var data = Utils.randomTextToLong(size);
		var key1 = "foo123bar";
		var key2 = "bar321foo";
		var key3 = "bar321foo2";
		client.putObject(bucketName, key1, data);

		var response = client.getObject(bucketName, key1);
		var versionId = response.getObjectMetadata().getVersionId();

		client.copyObject(new CopyObjectRequest(bucketName, key1, bucketName, key2).withSourceVersionId(versionId));
		response = client.getObject(bucketName, key2);
		var body = getBody(response.getObjectContent());
		assertEquals(data, body);
		assertEquals(size, response.getObjectMetadata().getContentLength());

		var versionId2 = response.getObjectMetadata().getVersionId();
		client.copyObject(new CopyObjectRequest(bucketName, key2, bucketName, key3).withSourceVersionId(versionId2));
		response = client.getObject(bucketName, key3);
		body = getBody(response.getObjectContent());
		assertEquals(data, body);
		assertEquals(size, response.getObjectMetadata().getContentLength());

		var bucketName2 = createBucket(client);
		checkConfigureVersioningRetry(bucketName2, BucketVersioningConfiguration.ENABLED);
		var key4 = "bar321foo3";
		client.copyObject(new CopyObjectRequest(bucketName, key1, bucketName2, key4).withSourceVersionId(versionId));
		response = client.getObject(bucketName2, key4);
		body = getBody(response.getObjectContent());
		assertEquals(data, body);
		assertEquals(size, response.getObjectMetadata().getContentLength());

		var bucketName3 = createBucket(client);
		checkConfigureVersioningRetry(bucketName3, BucketVersioningConfiguration.ENABLED);
		var key5 = "bar321foo4";
		client.copyObject(new CopyObjectRequest(bucketName, key1, bucketName3, key5).withSourceVersionId(versionId));
		response = client.getObject(bucketName3, key5);
		body = getBody(response.getObjectContent());
		assertEquals(data, body);
		assertEquals(size, response.getObjectMetadata().getContentLength());

		var key6 = "foo123bar2";
		client.copyObject(bucketName3, key5, bucketName, key6);
		response = client.getObject(bucketName, key6);
		body = getBody(response.getObjectContent());
		assertEquals(data, body);
		assertEquals(size, response.getObjectMetadata().getContentLength());
	}

	@Test
	@Tag("Version")
	// [버킷이 버저닝 가능하고 오브젝트이름에 특수문자가 들어갔을 경우] 오브젝트 복사 성공 확인
	public void testObjectCopyVersionedUrlEncoding() {
		var client = getClient();
		var bucketName = createBucket(client);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		var sourceKey = "foo?bar";

		client.putObject(bucketName, sourceKey, "");
		var response = client.getObject(bucketName, sourceKey);

		var targetKey = "bar&foo";
		client.copyObject(new CopyObjectRequest(bucketName, sourceKey, bucketName, targetKey)
				.withSourceVersionId(response.getObjectMetadata().getVersionId()));
		client.getObject(bucketName, targetKey);
	}

	@Test
	@Tag("Multipart")
	// [버킷에 버저닝 설정] 멀티파트로 업로드된 오브젝트 복사 확인
	public void testObjectCopyVersioningMultipartUpload() {
		var client = getClient();
		var bucketName = createBucket(client);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		var size = 50 * MainData.MB;
		var key1 = "src_multipart";
		var contentType = "text/bla";

		var key1Metadata = new ObjectMetadata();
		key1Metadata.addUserMetadata("x-amz-meta-foo", "bar");
		key1Metadata.setContentType(contentType);

		var uploads = setupMultipartUpload(client, bucketName, key1, size, key1Metadata);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, key1, uploads.uploadId, uploads.parts));

		var response = client.getObjectMetadata(bucketName, key1);
		var key1Size = response.getContentLength();
		var key1VersionId = response.getVersionId();

		var key2 = "dst_multipart";
		client.copyObject(new CopyObjectRequest(bucketName, key1, bucketName, key2).withSourceVersionId(key1VersionId));
		response = client.getObjectMetadata(bucketName, key2);
		var key2VersionId = response.getVersionId();
		assertEquals(key1Size, response.getContentLength());
		assertEquals(key1Metadata.getUserMetadata(), response.getUserMetadata());
		assertEquals(contentType, response.getContentType());
		checkContentUsingRange(bucketName, key2, uploads.getBody(), MainData.MB);

		var key3 = "dst_multipart2";
		client.copyObject(new CopyObjectRequest(bucketName, key2, bucketName, key3).withSourceVersionId(key2VersionId));
		response = client.getObjectMetadata(bucketName, key3);
		assertEquals(key1Size, response.getContentLength());
		assertEquals(key1Metadata.getUserMetadata(), response.getUserMetadata());
		assertEquals(contentType, response.getContentType());
		checkContentUsingRange(bucketName, key3, uploads.getBody(), MainData.MB);

		var bucketName2 = createBucket(client);
		checkConfigureVersioningRetry(bucketName2, BucketVersioningConfiguration.ENABLED);
		var key4 = "dst_multipart3";
		client.copyObject(
				new CopyObjectRequest(bucketName, key1, bucketName2, key4).withSourceVersionId(key1VersionId));
		response = client.getObjectMetadata(bucketName2, key4);
		assertEquals(key1Size, response.getContentLength());
		assertEquals(key1Metadata.getUserMetadata(), response.getUserMetadata());
		assertEquals(contentType, response.getContentType());
		checkContentUsingRange(bucketName2, key4, uploads.getBody(), MainData.MB);

		var bucketName3 = createBucket(client);
		checkConfigureVersioningRetry(bucketName3, BucketVersioningConfiguration.ENABLED);
		var key5 = "dst_multipart4";
		client.copyObject(
				new CopyObjectRequest(bucketName, key1, bucketName3, key5).withSourceVersionId(key1VersionId));
		response = client.getObjectMetadata(bucketName3, key5);
		assertEquals(key1Size, response.getContentLength());
		assertEquals(key1Metadata.getUserMetadata(), response.getUserMetadata());
		assertEquals(contentType, response.getContentType());
		checkContentUsingRange(bucketName3, key5, uploads.getBody(), MainData.MB);

		var key6 = "dst_multipart5";
		client.copyObject(bucketName3, key5, bucketName, key6);
		response = client.getObjectMetadata(bucketName, key6);
		assertEquals(key1Size, response.getContentLength());
		assertEquals(key1Metadata.getUserMetadata(), response.getUserMetadata());
		assertEquals(contentType, response.getContentType());
		checkContentUsingRange(bucketName, key6, uploads.getBody(), MainData.MB);
	}

	@Test
	@Tag("If Match")
	// if match 값을 추가하여 오브젝트를 복사할 경우 성공확인
	public void testCopyObjectIfMatchGood() {
		var client = getClient();
		var bucketName = createBucket(client);

		var putResponse = client.putObject(bucketName, "foo", "bar");

		client.copyObject(new CopyObjectRequest(bucketName, "foo", bucketName, "bar")
				.withMatchingETagConstraint(putResponse.getETag()));
		var getResponse = client.getObject(bucketName, "bar");
		var body = getBody(getResponse.getObjectContent());
		assertEquals("bar", body);
	}

	@Test
	@Tag("If Match")
	// if match에 잘못된 값을 입력하여 오브젝트를 복사할 경우 실패 확인
	public void testCopyObjectIfMatchFailed() {
		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(bucketName, "foo", "bar");

		var result = client.copyObject(
				new CopyObjectRequest(bucketName, "foo", bucketName, "bar").withMatchingETagConstraint("ABC"));
		assertNull(result);
	}

	@Test
	@Tag("encryption")
	// [source obj : normal, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공 확인
	public void testCopyNorSrcToNorBucketAndObj() {
		testObjectCopy(false, false, false, false, 1024);
		testObjectCopy(false, false, false, false, 256 * 1024);
		testObjectCopy(false, false, false, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	// [source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공
	// 확인
	public void testCopyNorSrcToNorBucketEncryptionObj() {
		testObjectCopy(false, false, false, true, 1024);
		testObjectCopy(false, false, false, true, 256 * 1024);
		testObjectCopy(false, false, false, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	// [source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공
	// 확인
	public void testCopyNorSrcToEncryptionBucketNorObj() {
		testObjectCopy(false, false, true, false, 1024);
		testObjectCopy(false, false, true, false, 256 * 1024);
		testObjectCopy(false, false, true, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	// [source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트
	// 복사 성공 확인
	public void testCopyNorSrcToEncryptionBucketAndObj() {
		testObjectCopy(false, false, true, true, 1024);
		testObjectCopy(false, false, true, true, 256 * 1024);
		testObjectCopy(false, false, true, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	// [source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공
	// 확인
	public void testCopyEncryptionSrcToNorBucketAndObj() {
		testObjectCopy(true, false, false, false, 1024);
		testObjectCopy(true, false, false, false, 256 * 1024);
		testObjectCopy(true, false, false, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	// [source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트
	// 복사 성공 확인
	public void testCopyEncryptionSrcToNorBucketEncryptionObj() {
		testObjectCopy(true, false, false, true, 1024);
		testObjectCopy(true, false, false, true, 256 * 1024);
		testObjectCopy(true, false, false, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	// [source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트
	// 복사 성공 확인
	public void testCopyEncryptionSrcToEncryptionBucketNorObj() {
		testObjectCopy(true, false, true, false, 1024);
		testObjectCopy(true, false, true, false, 256 * 1024);
		testObjectCopy(true, false, true, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	// [source obj : encryption, dest bucket : encryption, dest obj : encryption]
	// 오브젝트 복사 성공 확인
	public void testCopyEncryptionSrcToEncryptionBucketAndObj() {
		testObjectCopy(true, false, true, true, 1024);
		testObjectCopy(true, false, true, true, 256 * 1024);
		testObjectCopy(true, false, true, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	// [source bucket : encryption, source obj : normal, dest bucket : normal, dest
	// obj : normal] 오브젝트 복사 성공 확인
	public void testCopyEncryptionBucketNorObjToNorBucketAndObj() {
		testObjectCopy(false, true, false, false, 1024);
		testObjectCopy(false, true, false, false, 256 * 1024);
		testObjectCopy(false, true, false, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	// [source obj : normal, dest bucket : normal, dest obj : encryption] 오브젝트 복사 성공
	// 확인
	public void testCopyEncryptionBucketNorObjToNorBucketEncryptionObj() {
		testObjectCopy(false, true, false, true, 1024);
		testObjectCopy(false, true, false, true, 256 * 1024);
		testObjectCopy(false, true, false, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	// [source obj : normal, dest bucket : encryption, dest obj : normal] 오브젝트 복사 성공
	// 확인
	public void testCopyEncryptionBucketNorObjToEncryptionBucketNorObj() {
		testObjectCopy(false, true, true, false, 1024);
		testObjectCopy(false, true, true, false, 256 * 1024);
		testObjectCopy(false, true, true, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	// [source obj : normal, dest bucket : encryption, dest obj : encryption] 오브젝트
	// 복사 성공 확인
	public void testCopyEncryptionBucketNorObjToEncryptionBucketAndObj() {
		testObjectCopy(false, true, true, true, 1024);
		testObjectCopy(false, true, true, true, 256 * 1024);
		testObjectCopy(false, true, true, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	// [source obj : encryption, dest bucket : normal, dest obj : normal] 오브젝트 복사 성공
	// 확인
	public void testCopyEncryptionBucketAndObjToNorBucketAndObj() {
		testObjectCopy(true, true, false, false, 1024);
		testObjectCopy(true, true, false, false, 256 * 1024);
		testObjectCopy(true, true, false, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	// [source obj : encryption, dest bucket : normal, dest obj : encryption] 오브젝트
	// 복사 성공 확인
	public void testCopyEncryptionBucketAndObjToNorBucketEncryptionObj() {
		testObjectCopy(true, true, false, true, 1024);
		testObjectCopy(true, true, false, true, 256 * 1024);
		testObjectCopy(true, true, false, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	// [source obj : encryption, dest bucket : encryption, dest obj : normal] 오브젝트
	// 복사 성공 확인
	public void testCopyEncryptionBucketAndObjToEncryptionBucketNorObj() {
		testObjectCopy(true, true, true, false, 1024);
		testObjectCopy(true, true, true, false, 256 * 1024);
		testObjectCopy(true, true, true, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	// [source obj : encryption, dest bucket : encryption, dest obj : encryption]
	// 오브젝트 복사 성공 확인
	public void testCopyEncryptionBucketAndObjToEncryptionBucketAndObj() {
		testObjectCopy(true, true, true, true, 1024);
		testObjectCopy(true, true, true, true, 256 * 1024);
		testObjectCopy(true, true, true, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	// 일반 오브젝트에서 다양한 방식으로 복사 성공 확인
	public void testCopyToNormalSource() {
		var size1 = 1024;
		var size2 = 256 * 1024;
		var size3 = 1024 * 1024;

		testObjectCopy(EncryptionType.NORMAL, EncryptionType.NORMAL, size1);
		testObjectCopy(EncryptionType.NORMAL, EncryptionType.NORMAL, size2);
		testObjectCopy(EncryptionType.NORMAL, EncryptionType.NORMAL, size3);

		testObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_S3, size1);
		testObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_S3, size2);
		testObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_S3, size3);

		testObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_C, size1);
		testObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_C, size2);
		testObjectCopy(EncryptionType.NORMAL, EncryptionType.SSE_C, size3);
	}

	@Test
	@Tag("encryption")
	// SSE-S3암호화 된 오브젝트에서 다양한 방식으로 복사 성공 확인
	public void testCopyToSseS3Source() {
		var size1 = 1024;
		var size2 = 256 * 1024;
		var size3 = 1024 * 1024;

		testObjectCopy(EncryptionType.SSE_S3, EncryptionType.NORMAL, size1);
		testObjectCopy(EncryptionType.SSE_S3, EncryptionType.NORMAL, size2);
		testObjectCopy(EncryptionType.SSE_S3, EncryptionType.NORMAL, size3);

		testObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_S3, size1);
		testObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_S3, size2);
		testObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_S3, size3);

		testObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_C, size1);
		testObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_C, size2);
		testObjectCopy(EncryptionType.SSE_S3, EncryptionType.SSE_C, size3);
	}

	@Test
	@Tag("encryption")
	// SSE-C암호화 된 오브젝트에서 다양한 방식으로 복사 성공 확인
	public void testCopyToSseCSource() {
		var size1 = 1024;
		var size2 = 256 * 1024;
		var size3 = 1024 * 1024;

		testObjectCopy(EncryptionType.SSE_C, EncryptionType.NORMAL, size1);
		testObjectCopy(EncryptionType.SSE_C, EncryptionType.NORMAL, size2);
		testObjectCopy(EncryptionType.SSE_C, EncryptionType.NORMAL, size3);

		testObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_S3, size1);
		testObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_S3, size2);
		testObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_S3, size3);

		testObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_C, size1);
		testObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_C, size2);
		testObjectCopy(EncryptionType.SSE_C, EncryptionType.SSE_C, size3);
	}

	@Test
	@Tag("ERROR")
	// 삭제된 오브젝트 복사 실패 확인
	public void testCopyToDeletedObject() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key1 = "foo123bar";
		var ker2 = "bar321foo";

		client.putObject(bucketName, key1, key1);
		client.deleteObject(bucketName, key1);

		var e = assertThrows(AmazonServiceException.class, () -> client.copyObject(bucketName, key1, bucketName, ker2));
		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();
		assertEquals(404, statusCode);
		assertEquals("NoSuchKey", errorCode);
	}

	@Test
	@Tag("ERROR")
	// 버저닝된 버킷에서 삭제된 오브젝트 복사 실패 확인
	public void testCopyToDeleteMarkerObject() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key1 = "foo123bar";
		var ker2 = "bar321foo";

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		client.putObject(bucketName, key1, key1);
		client.deleteObject(bucketName, key1);

		var e = assertThrows(AmazonServiceException.class, () -> client.copyObject(bucketName, key1, bucketName, ker2));
		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();
		assertEquals(404, statusCode);
		assertEquals("NoSuchKey", errorCode);
	}

	@Test
	@Tag("Overwrite")
	// 복사할 오브젝트와 복사될 오브젝트의 경로가 같지만 메타데이터를 덮어쓰기 모드로 추가하면 해당 오브젝트의 메타데이터가 업데이트되는지
	// 확인(Versioning 설정)
	public void testObjectVersioningCopyToItselfWithMetadata() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo123bar";

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		client.putObject(bucketName, key, "foo");

		var metaData = new ObjectMetadata();
		metaData.addUserMetadata("foo", "bar");

		client.copyObject(new CopyObjectRequest(bucketName, key, bucketName, key).withNewObjectMetadata(metaData)
				.withMetadataDirective(MetadataDirective.REPLACE));
		var response = client.getObject(bucketName, key);

		assertEquals(metaData.getUserMetadata(), response.getObjectMetadata().getUserMetadata());
	}

	@Test
	@Tag("Overwrite")
	// 복사할 오브젝트와 복사될 오브젝트의 경로가 같지만 메타데이터를 덮어쓰기 모드로 변경하면 해당 오브젝트의 메타데이터가 업데이트되는지 확인
	public void testObjectCopyToItselfWithMetadataOverwrite() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo123bar";
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("foo", "bar");

		client.putObject(bucketName, key, createBody(key), metadata);
		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(metadata.getUserMetadata(), response.getUserMetadata());

		metadata.addUserMetadata("foo", "bar2");
		client.copyObject(new CopyObjectRequest(bucketName, key, bucketName, key).withNewObjectMetadata(metadata)
				.withMetadataDirective(MetadataDirective.REPLACE));
		response = client.getObjectMetadata(bucketName, key);

		assertEquals(metadata.getUserMetadata(), response.getUserMetadata());
	}

	@Test
	@Tag("Overwrite")
	// 복사할 오브젝트와 복사될 오브젝트의 경로가 같지만 메타데이터를 덮어쓰기 모드로 변경하면 해당 오브젝트의 메타데이터가 업데이트되는지
	// 확인(Versioning 설정)
	public void testObjectVersioningCopyToItselfWithMetadataOverwrite() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo123bar";
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("foo", "bar");

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		client.putObject(bucketName, key, createBody(key), metadata);
		var response = client.getObjectMetadata(bucketName, key);
		assertEquals(metadata.getUserMetadata(), response.getUserMetadata());

		metadata.addUserMetadata("foo", "bar2");
		client.copyObject(new CopyObjectRequest(bucketName, key, bucketName, key).withNewObjectMetadata(metadata)
				.withMetadataDirective(MetadataDirective.REPLACE));
		response = client.getObjectMetadata(bucketName, key);

		assertEquals(metadata.getUserMetadata(), response.getUserMetadata());
	}

	@Disabled("SDK v1에서는 알고리즘을 누락해도 기본값이 적용되어 에러가 발생하지 않음")
	@Test
	@Tag("ERROR")
	// sse-c로 암호화된 오브젝트를 복사할때 Algorithm을 누락해도 정상적으로 동작하는지 확인
	public void testCopyRevokeSseAlgorithm() {
		var client = getClientHttps();
		var bucketName = createBucket(client);
		var sourceKey = "source";
		var targetKey = "target";
		var data = Utils.randomTextToLong(1024);
		var metadata = new ObjectMetadata();

		var putSseC = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
				.withAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
				.withMd5("DWygnHRtgiJ77HCm+1rvHw==");
		var copySseC = new SSECustomerKey("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=")
				.withMd5("DWygnHRtgiJ77HCm+1rvHw==");

		client.putObject(
				new PutObjectRequest(bucketName, sourceKey, createBody(data), metadata).withSSECustomerKey(putSseC));

		client.copyObject(
				new CopyObjectRequest(bucketName, sourceKey, bucketName, targetKey).withSourceSSECustomerKey(copySseC));

		var response = client.getObject(bucketName, targetKey);
		var body = getBody(response.getObjectContent());
		assertEquals(data, body);
	}
}
