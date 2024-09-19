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
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hc.core5.http.HttpStatus;
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
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
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
	public void testObjectCopyZeroSize() {
		var source = "testObjectCopyZeroSizeSource";
		var target = "testObjectCopyZeroSizeTarget";
		var client = getClient();
		var bucketName = createObjects(client, source);

		client.putObject(bucketName, source, "");

		client.copyObject(bucketName, source, bucketName, target);

		var response = client.getObject(bucketName, target);
		assertEquals(0, response.getObjectMetadata().getContentLength());
	}

	@Test
	@Tag("Check")
	public void testObjectCopySameBucket() {
		var client = getClient();
		var bucketName = createBucket(client);
		var source = "testObjectCopySameBucketSource";
		var target = "testObjectCopySameBucketTarget";

		client.putObject(bucketName, source, source);

		client.copyObject(bucketName, source, bucketName, target);

		var response = client.getObject(bucketName, target);
		var body = getBody(response.getObjectContent());
		assertEquals(source, body);
	}

	@Test
	@Tag("ContentType")
	public void testObjectCopyVerifyContentType() {
		var client = getClient();
		var bucketName = createBucket(client);
		var source = "testObjectCopyVerifyContentTypeSource";
		var target = "testObjectCopyVerifyContentTypeTarget";
		var metadata = new ObjectMetadata();
		var contentType = "text/bla";
		metadata.setContentType(contentType);

		client.putObject(new PutObjectRequest(bucketName, source, createBody(source), metadata));
		client.copyObject(bucketName, source, bucketName, target);

		var response = client.getObject(bucketName, target);
		var body = getBody(response.getObjectContent());
		assertEquals(source, body);
		assertEquals(contentType, response.getObjectMetadata().getContentType());
	}

	@Test
	@Tag("Overwrite")
	public void testObjectCopyToItself() {
		var client = getClient();
		var bucketName = createBucket(client);
		var source = "testObjectCopyToItself";

		client.putObject(bucketName, source, "");

		var e = assertThrows(AmazonServiceException.class,
				() -> client.copyObject(bucketName, source, bucketName, source));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_REQUEST, e.getErrorCode());
	}

	@Test
	@Tag("Overwrite")
	public void testObjectCopyToItselfWithMetadata() {
		var client = getClient();
		var bucketName = createBucket(client);
		var source = "testObjectCopyToItselfWithMetadata";

		client.putObject(bucketName, source, source);

		var metaData = new ObjectMetadata();
		metaData.addUserMetadata("foo", "bar");

		client.copyObject(new CopyObjectRequest(bucketName, source, bucketName, source).withNewObjectMetadata(metaData)
				.withMetadataDirective(MetadataDirective.REPLACE));
		var response = client.getObject(bucketName, source);

		assertEquals(metaData.getUserMetadata(), response.getObjectMetadata().getUserMetadata());
	}

	@Test
	@Tag("Check")
	public void testObjectCopyDiffBucket() {
		var client = getClient();
		var sourceBucket = createBucket(client);
		var targetBucket = createBucket(client);
		var source = "testObjectCopyDiffBucketSource";
		var target = "testObjectCopyDiffBucketTarget";

		client.putObject(sourceBucket, source, source);

		client.copyObject(sourceBucket, source, targetBucket, target);

		var response = client.getObject(targetBucket, target);
		assertEquals(source, getBody(response.getObjectContent()));
	}

	@Test
	@Tag("Check")
	public void testObjectCopyNotOwnedBucket() {
		var client = getClient();
		var altClient = getAltClient();
		var sourceBucket = createBucket(client);
		var targetBucket = createBucket(altClient);
		var source = "testObjectCopyNotOwnedBucketSource";
		var target = "testObjectCopyNotOwnedBucketTarget";

		client.putObject(sourceBucket, source, source);

		var e = assertThrows(AmazonServiceException.class,
				() -> altClient.copyObject(sourceBucket, source, targetBucket, target));

		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		altClient.deleteBucket(targetBucket);
		deleteBucketList(targetBucket);
	}

	@Test
	@Tag("Check")
	public void testObjectCopyNotOwnedObjectBucket() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucketCannedAcl(client);
		var source = "testObjectCopyNotOwnedObjectBucketSource";
		var target = "testObjectCopyNotOwnedObjectBucketTarget";

		client.putObject(bucketName, source, source);

		var acl = createAltAcl(Permission.FullControl);

		client.setBucketAcl(bucketName, acl);
		client.setObjectAcl(bucketName, source, acl);

		altClient.getObject(bucketName, source);

		altClient.copyObject(bucketName, source, bucketName, target);
	}

	@Test
	@Tag("Overwrite")
	public void testObjectCopyCannedAcl() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucketCannedAcl(client);
		var source = "testObjectCopyCannedAclSource";
		var target = "testObjectCopyCannedAclTarget";

		client.putObject(bucketName, source, source);

		client.copyObject(new CopyObjectRequest(bucketName, source, bucketName, target)
				.withCannedAccessControlList(CannedAccessControlList.PublicRead));
		altClient.getObject(bucketName, target);

		var metaData = new ObjectMetadata();
		metaData.addUserMetadata("x-amz-meta-abc", "def");

		client.copyObject(new CopyObjectRequest(bucketName, target, bucketName, source)
				.withCannedAccessControlList(CannedAccessControlList.PublicRead)
				.withNewObjectMetadata(metaData)
				.withMetadataDirective(MetadataDirective.REPLACE));
		var response = altClient.getObject(bucketName, source);

		assertEquals(source, getBody(response.getObjectContent()));
		assertEquals(metaData.getUserMetadata(), response.getObjectMetadata().getUserMetadata());
	}

	@Test
	@Tag("Check")
	public void testObjectCopyRetainingMetadata() {
		var client = getClient();
		for (var size : new int[] { 3, 1024 * 1024 }) {
			var bucketName = createBucket(client);
			var contentType = "audio/ogg";

			var source = "testObjectCopyRetainingMetadataSource";
			var target = "testObjectCopyRetainingMetadataTarget";

			var metaData = new ObjectMetadata();
			metaData.addUserMetadata("x-amz-meta-source", "value1");
			metaData.addUserMetadata("x-amz-meta-target", "value2");
			metaData.setContentType(contentType);
			metaData.setContentLength(size);

			client.putObject(
					new PutObjectRequest(bucketName, source, createBody(Utils.randomTextToLong(size)), metaData));
			client.copyObject(bucketName, source, bucketName, target);

			var response = client.getObject(bucketName, target);
			assertEquals(contentType, response.getObjectMetadata().getContentType());
			assertEquals(metaData.getUserMetadata(), response.getObjectMetadata().getUserMetadata());
			assertEquals(size, response.getObjectMetadata().getContentLength());
		}
	}

	@Test
	@Tag("Check")
	public void testObjectCopyReplacingMetadata() {
		var client = getClient();
		for (var size : new int[] { 3, 1024 * 1024 }) {
			var bucketName = createBucket(client);
			var contentType = "audio/ogg";

			var source = "testObjectCopyReplacingMetadataSource";
			var target = "testObjectCopyReplacingMetadataTarget";

			var metadata = new ObjectMetadata();
			metadata.addUserMetadata("x-amz-meta-source", "value1");
			metadata.addUserMetadata("x-amz-meta-target", "value2");
			metadata.setContentType(contentType);
			metadata.setContentLength(size);

			client.putObject(
					new PutObjectRequest(bucketName, source, createBody(Utils.randomTextToLong(size)), metadata));

			var metadata2 = new ObjectMetadata();
			metadata2.addUserMetadata("x-amz-meta-key3", "value3");
			metadata2.addUserMetadata("x-amz-meta-key4", "value4");
			metadata2.setContentType(contentType);

			client.copyObject(
					new CopyObjectRequest(bucketName, source, bucketName, target).withNewObjectMetadata(metadata2)
							.withMetadataDirective(MetadataDirective.REPLACE));

			var response = client.getObject(bucketName, target);
			assertEquals(contentType, response.getObjectMetadata().getContentType());
			assertEquals(metadata2.getUserMetadata(), response.getObjectMetadata().getUserMetadata());
			assertEquals(size, response.getObjectMetadata().getContentLength());
		}
	}

	@Test
	@Tag("ERROR")
	public void testObjectCopyBucketNotFound() {
		var client = getClient();
		var bucketName = createBucket(client);

		var e = assertThrows(AmazonServiceException.class,
				() -> client.copyObject(bucketName + "-fake", "testObjectCopyBucketNotFoundSource", bucketName,
						"testObjectCopyBucketNotFoundTarget"));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testObjectCopyKeyNotFound() {
		var client = getClient();
		var bucketName = createBucket(client);

		var e = assertThrows(AmazonServiceException.class,
				() -> client.copyObject(bucketName, "testObjectCopyKeyNotFoundSource", bucketName,
						"testObjectCopyKeyNotFoundTarget"));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.getErrorCode());
	}

	@Test
	@Tag("Version")
	public void testObjectCopyVersionedBucket() {
		var client = getClient();
		var bucketName = createBucket(client);
		var size = 1 * 5;
		var data = Utils.randomTextToLong(size);
		var source = "testObjectCopyVersionedBucketSource";
		var target = "testObjectCopyVersionedBucketTarget";

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		client.putObject(bucketName, source, data);

		var response = client.getObject(bucketName, source);
		var versionId = response.getObjectMetadata().getVersionId();

		client.copyObject(new CopyObjectRequest(bucketName, source, bucketName, target).withSourceVersionId(versionId));

		response = client.getObject(bucketName, target);
		var targetVid = response.getObjectMetadata().getVersionId();

		assertEquals(data, getBody(response.getObjectContent()));
		assertEquals(size, response.getObjectMetadata().getContentLength());

		var target2 = "testObjectCopyVersionedBucketTarget2";
		client.copyObject(new CopyObjectRequest(bucketName, target, bucketName, target2)
				.withSourceVersionId(targetVid));
		response = client.getObject(bucketName, target2);

		assertEquals(data, getBody(response.getObjectContent()));
		assertEquals(size, response.getObjectMetadata().getContentLength());

		var targetBucket = createBucket(client);
		checkConfigureVersioningRetry(targetBucket, BucketVersioningConfiguration.ENABLED);
		var target3 = "testObjectCopyVersionedBucketTarget3";

		client.copyObject(new CopyObjectRequest(bucketName, source, targetBucket, target3)
				.withSourceVersionId(versionId));

		response = client.getObject(targetBucket, target3);
		assertEquals(data, getBody(response.getObjectContent()));
		assertEquals(size, response.getObjectMetadata().getContentLength());

		var bucketName3 = createBucket(client);
		checkConfigureVersioningRetry(bucketName3, BucketVersioningConfiguration.ENABLED);
		var target4 = "testObjectCopyVersionedBucketTarget4";
		client.copyObject(new CopyObjectRequest(bucketName, source, bucketName3, target4)
				.withSourceVersionId(versionId));

		response = client.getObject(bucketName3, target4);
		assertEquals(data, getBody(response.getObjectContent()));
		assertEquals(size, response.getObjectMetadata().getContentLength());

		var target5 = "testObjectCopyVersionedBucketTarget5";
		client.copyObject(bucketName3, target4, bucketName, target5);
		response = client.getObject(bucketName, target5);
		assertEquals(data, getBody(response.getObjectContent()));
		assertEquals(size, response.getObjectMetadata().getContentLength());
	}

	@Test
	@Tag("Version")
	public void testObjectCopyVersionedUrlEncoding() {
		var client = getClient();
		var bucketName = createBucket(client);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		var source = "testObjectCopyVersionedUrlEncoding?Source";

		client.putObject(bucketName, source, source);
		var response = client.getObject(bucketName, source);
		var versionId = response.getObjectMetadata().getVersionId();
		assertEquals(source, getBody(response.getObjectContent()));

		var target = "testObjectCopyVersionedUrlEncoding&Target";
		client.copyObject(new CopyObjectRequest(bucketName, source, bucketName, target)
				.withSourceVersionId(versionId));
		response = client.getObject(bucketName, target);
		assertEquals(source, getBody(response.getObjectContent()));
	}

	@Test
	@Tag("Multipart")
	public void testObjectCopyVersioningMultipartUpload() {
		var client = getClient();
		var bucketName = createBucket(client);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		var size = 50 * MainData.MB;
		var source = "testObjectCopyVersioningMultipartUploadSource";

		var sourceMetadata = new ObjectMetadata();
		sourceMetadata.addUserMetadata("x-amz-meta-foo", "bar");

		var uploads = setupMultipartUpload(client, bucketName, source, size, sourceMetadata);
		client.completeMultipartUpload(
				new CompleteMultipartUploadRequest(bucketName, source, uploads.uploadId, uploads.parts));

		var response = client.getObjectMetadata(bucketName, source);
		var sourceSize = response.getContentLength();
		var sourceVid = response.getVersionId();

		var target = "testObjectCopyVersioningMultipartUploadTarget";
		client.copyObject(new CopyObjectRequest(bucketName, source, bucketName, target)
				.withSourceVersionId(sourceVid));

		response = client.getObjectMetadata(bucketName, target);
		var targetVid = response.getVersionId();

		assertEquals(sourceSize, response.getContentLength());
		assertEquals(sourceMetadata.getUserMetadata(), response.getUserMetadata());
		checkContentUsingRange(bucketName, target, uploads.getBody(), MainData.MB);

		var target2 = "testObjectCopyVersioningMultipartUploadTarget2";
		client.copyObject(new CopyObjectRequest(bucketName, target, bucketName, target2)
				.withSourceVersionId(targetVid));
		response = client.getObjectMetadata(bucketName, target2);
		assertEquals(sourceSize, response.getContentLength());
		assertEquals(sourceMetadata.getUserMetadata(), response.getUserMetadata());
		checkContentUsingRange(bucketName, target2, uploads.getBody(), MainData.MB);

		var targetBucket = createBucket(client);
		checkConfigureVersioningRetry(targetBucket, BucketVersioningConfiguration.ENABLED);

		var target3 = "testObjectCopyVersioningMultipartUploadTarget3";
		client.copyObject(
				new CopyObjectRequest(bucketName, source, targetBucket, target3).withSourceVersionId(sourceVid));
		response = client.getObjectMetadata(targetBucket, target3);
		assertEquals(sourceSize, response.getContentLength());
		assertEquals(sourceMetadata.getUserMetadata(), response.getUserMetadata());
		checkContentUsingRange(targetBucket, target3, uploads.getBody(), MainData.MB);

		var bucketName3 = createBucket(client);
		checkConfigureVersioningRetry(bucketName3, BucketVersioningConfiguration.ENABLED);

		var target4 = "testObjectCopyVersioningMultipartUploadTarget4";
		client.copyObject(
				new CopyObjectRequest(bucketName, source, bucketName3, target4).withSourceVersionId(sourceVid));
		response = client.getObjectMetadata(bucketName3, target4);
		assertEquals(sourceSize, response.getContentLength());
		assertEquals(sourceMetadata.getUserMetadata(), response.getUserMetadata());
		checkContentUsingRange(bucketName3, target4, uploads.getBody(), MainData.MB);

		var target5 = "testObjectCopyVersioningMultipartUploadTarget5";
		client.copyObject(bucketName3, target4, bucketName, target5);
		response = client.getObjectMetadata(bucketName, target5);
		assertEquals(sourceSize, response.getContentLength());
		assertEquals(sourceMetadata.getUserMetadata(), response.getUserMetadata());
		checkContentUsingRange(bucketName, target5, uploads.getBody(), MainData.MB);
	}

	@Test
	@Tag("If Match")
	public void testCopyObjectIfMatchGood() {
		var client = getClient();
		var bucketName = createBucket(client);
		var source = "testCopyObjectIfMatchGoodSource";
		var target = "testCopyObjectIfMatchGoodTarget";

		var eTag = client.putObject(bucketName, source, source).getETag();

		client.copyObject(new CopyObjectRequest(bucketName, source, bucketName, target)
				.withMatchingETagConstraint(eTag));
		var response = client.getObject(bucketName, target);
		assertEquals(source, getBody(response.getObjectContent()));
	}

	@Test
	@Tag("If Match")
	public void testCopyObjectIfMatchFailed() {
		var client = getClient();
		var bucketName = createBucket(client);
		var source = "testCopyObjectIfMatchFailedSource";
		var target = "testCopyObjectIfMatchFailedTarget";

		client.putObject(bucketName, source, source);

		var e = assertThrows(AmazonServiceException.class,
				() -> client.copyObject(new CopyObjectRequest(bucketName, target, bucketName, target)
						.withMatchingETagConstraint("ABC")));

		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.getErrorCode());
	}

	@Test
	@Tag("encryption")
	public void testCopyNorSrcToNorBucketAndObj() {
		testObjectCopy(false, false, false, false, 1024);
		testObjectCopy(false, false, false, false, 256 * 1024);
		testObjectCopy(false, false, false, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyNorSrcToNorBucketEncryptionObj() {
		testObjectCopy(false, false, false, true, 1024);
		testObjectCopy(false, false, false, true, 256 * 1024);
		testObjectCopy(false, false, false, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyNorSrcToEncryptionBucketNorObj() {
		testObjectCopy(false, false, true, false, 1024);
		testObjectCopy(false, false, true, false, 256 * 1024);
		testObjectCopy(false, false, true, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyNorSrcToEncryptionBucketAndObj() {
		testObjectCopy(false, false, true, true, 1024);
		testObjectCopy(false, false, true, true, 256 * 1024);
		testObjectCopy(false, false, true, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionSrcToNorBucketAndObj() {
		testObjectCopy(true, false, false, false, 1024);
		testObjectCopy(true, false, false, false, 256 * 1024);
		testObjectCopy(true, false, false, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionSrcToNorBucketEncryptionObj() {
		testObjectCopy(true, false, false, true, 1024);
		testObjectCopy(true, false, false, true, 256 * 1024);
		testObjectCopy(true, false, false, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionSrcToEncryptionBucketNorObj() {
		testObjectCopy(true, false, true, false, 1024);
		testObjectCopy(true, false, true, false, 256 * 1024);
		testObjectCopy(true, false, true, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionSrcToEncryptionBucketAndObj() {
		testObjectCopy(true, false, true, true, 1024);
		testObjectCopy(true, false, true, true, 256 * 1024);
		testObjectCopy(true, false, true, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionBucketNorObjToNorBucketAndObj() {
		testObjectCopy(false, true, false, false, 1024);
		testObjectCopy(false, true, false, false, 256 * 1024);
		testObjectCopy(false, true, false, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionBucketNorObjToNorBucketEncryptionObj() {
		testObjectCopy(false, true, false, true, 1024);
		testObjectCopy(false, true, false, true, 256 * 1024);
		testObjectCopy(false, true, false, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionBucketNorObjToEncryptionBucketNorObj() {
		testObjectCopy(false, true, true, false, 1024);
		testObjectCopy(false, true, true, false, 256 * 1024);
		testObjectCopy(false, true, true, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionBucketNorObjToEncryptionBucketAndObj() {
		testObjectCopy(false, true, true, true, 1024);
		testObjectCopy(false, true, true, true, 256 * 1024);
		testObjectCopy(false, true, true, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionBucketAndObjToNorBucketAndObj() {
		testObjectCopy(true, true, false, false, 1024);
		testObjectCopy(true, true, false, false, 256 * 1024);
		testObjectCopy(true, true, false, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionBucketAndObjToNorBucketEncryptionObj() {
		testObjectCopy(true, true, false, true, 1024);
		testObjectCopy(true, true, false, true, 256 * 1024);
		testObjectCopy(true, true, false, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionBucketAndObjToEncryptionBucketNorObj() {
		testObjectCopy(true, true, true, false, 1024);
		testObjectCopy(true, true, true, false, 256 * 1024);
		testObjectCopy(true, true, true, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionBucketAndObjToEncryptionBucketAndObj() {
		testObjectCopy(true, true, true, true, 1024);
		testObjectCopy(true, true, true, true, 256 * 1024);
		testObjectCopy(true, true, true, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
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
	public void testCopyToDeletedObject() {
		var client = getClient();
		var bucketName = createBucket(client);
		var source = "foo123bar";
		var ker2 = "bar321foo";

		client.putObject(bucketName, source, source);
		client.deleteObject(bucketName, source);

		var e = assertThrows(AmazonServiceException.class,
				() -> client.copyObject(bucketName, source, bucketName, ker2));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testCopyToDeleteMarkerObject() {
		var client = getClient();
		var bucketName = createBucket(client);
		var source = "testCopyToDeleteMarkerObjectSource";
		var target = "testCopyToDeleteMarkerObjectTarget";

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		client.putObject(bucketName, source, source);
		client.deleteObject(bucketName, source);

		var e = assertThrows(AmazonServiceException.class,
				() -> client.copyObject(bucketName, source, bucketName, target));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.getErrorCode());
	}

	@Test
	@Tag("Overwrite")
	public void testObjectVersioningCopyToItselfWithMetadata() {
		var client = getClient();
		var bucketName = createBucket(client);
		var source = "testObjectVersioningCopyToItselfWithMetadataSource";

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		client.putObject(bucketName, source, source);

		var metaData = new ObjectMetadata();
		metaData.addUserMetadata("foo", "bar");

		client.copyObject(new CopyObjectRequest(bucketName, source, bucketName, source).withNewObjectMetadata(metaData)
				.withMetadataDirective(MetadataDirective.REPLACE));
		var response = client.getObject(bucketName, source);

		assertEquals(metaData.getUserMetadata(), response.getObjectMetadata().getUserMetadata());
	}

	@Test
	@Tag("Overwrite")
	public void testObjectCopyToItselfWithMetadataOverwrite() {
		var client = getClient();
		var bucketName = createBucket(client);
		var source = "testObjectCopyToItselfWithMetadataOverwriteSource";
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("foo", "bar");

		client.putObject(bucketName, source, createBody(source), metadata);
		var response = client.getObjectMetadata(bucketName, source);
		assertEquals(metadata.getUserMetadata(), response.getUserMetadata());

		metadata.addUserMetadata("foo", "bar2");
		client.copyObject(new CopyObjectRequest(bucketName, source, bucketName, source).withNewObjectMetadata(metadata)
				.withMetadataDirective(MetadataDirective.REPLACE));
		response = client.getObjectMetadata(bucketName, source);

		assertEquals(metadata.getUserMetadata(), response.getUserMetadata());
	}

	@Test
	@Tag("Overwrite")
	public void testObjectVersioningCopyToItselfWithMetadataOverwrite() {
		var client = getClient();
		var bucketName = createBucket(client);
		var source = "testObjectVersioningCopyToItselfWithMetadataOverwriteSource";
		var metadata = new ObjectMetadata();
		metadata.addUserMetadata("foo", "bar");

		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);

		client.putObject(bucketName, source, createBody(source), metadata);
		var response = client.getObjectMetadata(bucketName, source);
		assertEquals(metadata.getUserMetadata(), response.getUserMetadata());

		metadata.addUserMetadata("foo", "bar2");
		client.copyObject(new CopyObjectRequest(bucketName, source, bucketName, source).withNewObjectMetadata(metadata)
				.withMetadataDirective(MetadataDirective.REPLACE));
		response = client.getObjectMetadata(bucketName, source);

		assertEquals(metadata.getUserMetadata(), response.getUserMetadata());
	}

	@Disabled("SDK v1에서는 알고리즘을 누락해도 기본값이 적용되어 에러가 발생하지 않음")
	@Test
	@Tag("ERROR")
	public void testCopyRevokeSseAlgorithm() {
		var client = getClientHttps();
		var bucketName = createBucket(client);
		var sourceKey = "testCopyRevokeSseAlgorithmSource";
		var targetKey = "testCopyRevokeSseAlgorithmTarget";
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
