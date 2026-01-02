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

import java.util.HashMap;
import java.util.List;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.Permission;
import software.amazon.awssdk.services.s3.model.Tagging;

@Execution(ExecutionMode.CONCURRENT)
public class CopyObject extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("CopyObject V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("CopyObject V2 End");
	}

	@Test
	@Tag("Check")
	public void testObjectCopyZeroSize() {
		var source = "testObjectCopyZeroSizeSource";
		var target = "testObjectCopyZeroSizeTarget";
		var client = getClient();
		var bucketName = createObjects(client, source);

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.empty());

		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(source)
				.destinationBucket(bucketName).destinationKey(target));

		var response = client.getObject(g -> g.bucket(bucketName).key(target));
		assertEquals(0, response.response().contentLength());
	}

	@Test
	@Tag("Check")
	public void testObjectCopySameBucket() {
		var client = getClient();
		var bucketName = createBucket(client);
		var source = "testObjectCopySameBucketSource";
		var target = "testObjectCopySameBucketTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));

		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
				.destinationBucket(bucketName).destinationKey(target));

		var response = client.getObject(g -> g.bucket(bucketName).key(target));
		assertEquals(source, getBody(response));
	}

	@Test
	@Tag("ContentType")
	public void testObjectCopyVerifyContentType() {
		var client = getClient();
		var bucketName = createBucket(client);
		var source = "testObjectCopyVerifyContentTypeSource";
		var target = "testObjectCopyVerifyContentTypeTarget";
		var contentType = "text/bla";

		client.putObject(p -> p.bucket(bucketName).key(source).contentType(contentType),
				RequestBody.fromString(source));
		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
				.destinationBucket(bucketName).destinationKey(target));

		var response = client.getObject(g -> g.bucket(bucketName).key(target));
		assertEquals(source, getBody(response));
		assertEquals(contentType, response.response().contentType());
	}

	@Test
	@Tag("Overwrite")
	public void testObjectCopyToItself() {
		var client = getClient();
		var bucketName = createBucket(client);
		var source = "testObjectCopyToItself";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.empty());

		var e = assertThrows(AwsServiceException.class, () -> client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(source).destinationBucket(bucketName).destinationKey(source)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_REQUEST, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Overwrite")
	public void testObjectCopyToItselfWithMetadata() {
		var client = getClient();
		var bucketName = createBucket(client);
		var source = "testObjectCopyToItselfWithMetadata";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));

		var metaData = new HashMap<String, String>();
		metaData.put("foo", "bar");

		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(source)
				.destinationBucket(bucketName).destinationKey(source)
				.metadataDirective(MetadataDirective.REPLACE)
				.metadata(metaData));
		var response = client.getObject(g -> g.bucket(bucketName).key(source));

		assertEquals(metaData, response.response().metadata());
	}

	@Test
	@Tag("Check")
	public void testObjectCopyDiffBucket() {
		var client = getClient();
		var sourceBucket = createBucket(client);
		var targetBucket = createBucket(client);
		var source = "testObjectCopyDiffBucketSource";
		var target = "testObjectCopyDiffBucketTarget";

		client.putObject(p -> p.bucket(sourceBucket).key(source), RequestBody.fromString(source));

		client.copyObject(c -> c.sourceBucket(sourceBucket).sourceKey(source)
				.destinationBucket(targetBucket).destinationKey(target));

		var response = client.getObject(g -> g.bucket(targetBucket).key(target));
		assertEquals(source, getBody(response));
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

		client.putObject(p -> p.bucket(sourceBucket).key(source), RequestBody.fromString(source));

		var e = assertThrows(AwsServiceException.class,
				() -> altClient.copyObject(c -> c.sourceBucket(sourceBucket).sourceKey(source)
						.destinationBucket(targetBucket).destinationKey(target)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		altClient.deleteBucket(d -> d.bucket(targetBucket));
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

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));

		var acl = createAltAcl(Permission.FULL_CONTROL);

		client.putBucketAcl(p -> p.bucket(bucketName).accessControlPolicy(acl));
		client.putObjectAcl(p -> p.bucket(bucketName).key(source).accessControlPolicy(acl));

		altClient.getObject(g -> g.bucket(bucketName).key(source));

		altClient.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
				.destinationBucket(bucketName).destinationKey(target));
	}

	@Test
	@Tag("Overwrite")
	public void testObjectCopyCannedAcl() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucketCannedAcl(client);
		var source = "testObjectCopyCannedAclSource";
		var target = "testObjectCopyCannedAclTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));

		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
				.destinationBucket(bucketName).destinationKey(target)
				.acl(ObjectCannedACL.PUBLIC_READ));
		altClient.getObject(g -> g.bucket(bucketName).key(target));

		var metaData = new HashMap<String, String>();
		metaData.put("abc", "def");

		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
				.destinationBucket(bucketName).destinationKey(target)
				.acl(ObjectCannedACL.PUBLIC_READ)
				.metadata(metaData)
				.metadataDirective(MetadataDirective.REPLACE));
		var response = altClient.getObject(g -> g.bucket(bucketName).key(target));

		assertEquals(source, getBody(response));
		assertEquals(metaData, response.response().metadata());
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

			var metadata = new HashMap<String, String>();
			metadata.put("source", "value1");
			metadata.put("target", "value2");

			client.putObject(p -> p.bucket(bucketName).key(source).metadata(metadata).contentType(contentType),
					RequestBody.fromString(Utils.randomTextToLong(size)));
			client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
					.destinationBucket(bucketName).destinationKey(target));

			var response = client.getObject(g -> g.bucket(bucketName).key(target));
			assertEquals(contentType, response.response().contentType());
			assertEquals(metadata, response.response().metadata());
			assertEquals(size, response.response().contentLength());
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

			var metadata = new HashMap<String, String>();
			metadata.put("source", "value1");
			metadata.put("target", "value2");

			client.putObject(p -> p.bucket(bucketName).key(source).metadata(metadata).contentType(contentType),
					RequestBody.fromString(Utils.randomTextToLong(size)));

			var response = client.getObject(g -> g.bucket(bucketName).key(source));
			assertEquals(contentType, response.response().contentType());
			assertEquals(metadata, response.response().metadata());

			var metadata2 = new HashMap<String, String>();
			metadata2.put("key3", "value3");
			metadata2.put("key4", "value4");

			client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
					.destinationBucket(bucketName).destinationKey(target)
					.metadata(metadata2).contentType(contentType)
					.metadataDirective(MetadataDirective.REPLACE));

			response = client.getObject(g -> g.bucket(bucketName).key(target));
			assertEquals(contentType, response.response().contentType());
			assertEquals(metadata2, response.response().metadata());
			assertEquals(size, response.response().contentLength());
		}
	}

	@Test
	@Tag("ERROR")
	public void testObjectCopyBucketNotFound() {
		var client = getClient();
		var bucketName = createBucket(client);
		var e = assertThrows(AwsServiceException.class,
				() -> client.copyObject(c -> c.sourceBucket(bucketName + "-fake").sourceKey("testObjectCopyBucketNotFoundSource")
						.destinationBucket(bucketName).destinationKey("testObjectCopyBucketNotFoundTarget")));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testObjectCopyKeyNotFound() {
		var client = getClient();
		var bucketName = createBucket(client);

		var e = assertThrows(AwsServiceException.class,
				() -> client.copyObject(c -> c.sourceBucket(bucketName).sourceKey("testObjectCopyKeyNotFoundSource")
						.destinationBucket(bucketName).destinationKey("testObjectCopyKeyNotFoundTarget")));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Version")
	public void testObjectCopyVersioningBucket() {
		var client = getClient();
		var bucketName = createBucket(client);
		var size = 1 * 5;
		var data = Utils.randomTextToLong(size);
		var source = "testObjectCopyVersionedBucketSource";
		var target = "testObjectCopyVersionedBucketTarget";

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(data));

		var response = client.getObject(g -> g.bucket(bucketName).key(source));
		var sourceVid = response.response().versionId();

		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source).sourceVersionId(sourceVid)
				.destinationBucket(bucketName).destinationKey(target));

		response = client.getObject(g -> g.bucket(bucketName).key(target));
		var targetVid = response.response().versionId();

		assertEquals(data, getBody(response));
		assertEquals(size, response.response().contentLength());

		var target2 = "testObjectCopyVersionedBucketTarget2";
		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(target).sourceVersionId(targetVid)
				.destinationBucket(bucketName).destinationKey(target2));

		response = client.getObject(g -> g.bucket(bucketName).key(target2));
		assertEquals(data, getBody(response));
		assertEquals(size, response.response().contentLength());

		var targetBucket = createBucket(client);
		checkConfigureVersioningRetry(targetBucket, BucketVersioningStatus.ENABLED);
		var target3 = "testObjectCopyVersionedBucketTarget3";

		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source).sourceVersionId(sourceVid)
				.destinationBucket(targetBucket).destinationKey(target3));

		response = client.getObject(g -> g.bucket(targetBucket).key(target3));
		assertEquals(data, getBody(response));
		assertEquals(size, response.response().contentLength());

		var bucketName3 = createBucket(client);
		checkConfigureVersioningRetry(bucketName3, BucketVersioningStatus.ENABLED);
		var target4 = "testObjectCopyVersionedBucketTarget4";
		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source).sourceVersionId(sourceVid)
				.destinationBucket(bucketName3).destinationKey(target4));

		response = client.getObject(g -> g.bucket(bucketName3).key(target4));
		assertEquals(data, getBody(response));
		assertEquals(size, response.response().contentLength());

		var target5 = "testObjectCopyVersionedBucketTarget5";
		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
				.destinationBucket(bucketName).destinationKey(target5));
		response = client.getObject(g -> g.bucket(bucketName).key(target5));
		assertEquals(data, getBody(response));
		assertEquals(size, response.response().contentLength());
	}

	@Test
	@Tag("Version")
	public void testObjectCopyVersioningUrlEncoding() {
		var client = getClient();
		var bucketName = createBucket(client);
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		var source = "testObjectCopyVersionedUrlEncoding?Source";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));
		var response = client.getObject(g -> g.bucket(bucketName).key(source));
		var versionId = response.response().versionId();
		assertEquals(source, getBody(response));

		var target = "testObjectCopyVersionedUrlEncoding&Target";
		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source).sourceVersionId(versionId)
				.destinationBucket(bucketName).destinationKey(target));
		response = client.getObject(g -> g.bucket(bucketName).key(target));
		assertEquals(source, getBody(response));
	}

	@Test
	@Tag("Multipart")
	public void testObjectCopyVersioningMultipartUpload() {
		var client = getClient();
		var bucketName = createBucket(client);
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		var size = 50 * MainData.MB;
		var source = "testObjectCopyVersioningMultipartUploadSource";

		var sourceMetadata = new HashMap<String, String>();
		sourceMetadata.put("foo", "bar");

		var uploads = setupMultipartUpload(client, bucketName, source, size, sourceMetadata);
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(source).uploadId(uploads.uploadId)
				.multipartUpload(p -> p.parts(uploads.parts)));

		var response = client.headObject(h -> h.bucket(bucketName).key(source));
		var sourceSize = response.contentLength();
		var sourceVid = response.versionId();

		var target = "testObjectCopyVersioningMultipartUploadTarget";
		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source).sourceVersionId(sourceVid)
				.destinationBucket(bucketName).destinationKey(target));

		response = client.headObject(h -> h.bucket(bucketName).key(target));
		var targetVid = response.versionId();

		assertEquals(sourceSize, response.contentLength());
		assertEquals(sourceMetadata, response.metadata());
		checkContentUsingRange(bucketName, target, uploads.getBody(), MainData.MB);

		var target2 = "testObjectCopyVersioningMultipartUploadTarget2";
		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(target).sourceVersionId(targetVid)
				.destinationBucket(bucketName).destinationKey(target2));
		response = client.headObject(h -> h.bucket(bucketName).key(target2));
		assertEquals(sourceSize, response.contentLength());
		assertEquals(sourceMetadata, response.metadata());
		checkContentUsingRange(bucketName, target2, uploads.getBody(), MainData.MB);

		var targetBucket = createBucket(client);
		checkConfigureVersioningRetry(targetBucket, BucketVersioningStatus.ENABLED);

		var target3 = "testObjectCopyVersioningMultipartUploadTarget3";
		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source).sourceVersionId(sourceVid)
				.destinationBucket(targetBucket).destinationKey(target3));
		response = client.headObject(h -> h.bucket(targetBucket).key(target3));
		assertEquals(sourceSize, response.contentLength());
		assertEquals(sourceMetadata, response.metadata());
		checkContentUsingRange(targetBucket, target3, uploads.getBody(), MainData.MB);

		var bucketName3 = createBucket(client);
		checkConfigureVersioningRetry(bucketName3, BucketVersioningStatus.ENABLED);

		var target4 = "testObjectCopyVersioningMultipartUploadTarget4";
		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source).sourceVersionId(sourceVid)
				.destinationBucket(bucketName3).destinationKey(target4));
		response = client.headObject(h -> h.bucket(bucketName3).key(target4));
		assertEquals(sourceSize, response.contentLength());
		assertEquals(sourceMetadata, response.metadata());
		checkContentUsingRange(bucketName3, target4, uploads.getBody(), MainData.MB);

		var target5 = "testObjectCopyVersioningMultipartUploadTarget5";
		client.copyObject(c -> c.sourceBucket(bucketName3).sourceKey(target4)
				.destinationBucket(bucketName).destinationKey(target5));
		response = client.headObject(h -> h.bucket(bucketName).key(target5));
		assertEquals(sourceSize, response.contentLength());
		assertEquals(sourceMetadata, response.metadata());
		checkContentUsingRange(bucketName, target5, uploads.getBody(), MainData.MB);
	}

	@Test
	@Tag("If Match")
	public void testCopyObjectIfMatchGood() {
		var client = getClient();
		var bucketName = createBucket(client);
		var source = "testCopyObjectIfMatchGoodSource";
		var target = "testCopyObjectIfMatchGoodTarget";

		var eTag = client.putObject(p -> p.bucket(bucketName).key(source),
				RequestBody.fromString(source)).eTag();

		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source).copySourceIfMatch(eTag)
				.destinationBucket(bucketName).destinationKey(target));
		var response = client.getObject(g -> g.bucket(bucketName).key(target));
		assertEquals(source, getBody(response));
	}

	@Test
	@Tag("If Match")
	public void testCopyObjectIfMatchFailed() {
		var client = getClient();
		var bucketName = createBucket(client);
		var source = "testCopyObjectIfMatchFailedSource";
		var target = "testCopyObjectIfMatchFailedTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));

		var e = assertThrows(AwsServiceException.class,
				() -> client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
						.destinationBucket(bucketName).destinationKey(target).copySourceIfMatch("ABC")));

		assertEquals(HttpStatus.SC_PRECONDITION_FAILED, e.statusCode());
		assertEquals(MainData.PRECONDITION_FAILED, e.awsErrorDetails().errorCode());
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
		var source = "testCopyToDeletedObjectSource";
		var target = "testCopyToDeletedObjectTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));
		client.deleteObject(d -> d.bucket(bucketName).key(source));

		var e = assertThrows(AwsServiceException.class,
				() -> client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
						.destinationBucket(bucketName).destinationKey(target)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testCopyToDeleteMarkerObject() {
		var client = getClient();
		var bucketName = createBucket(client);
		var source = "testCopyToDeleteMarkerObjectSource";
		var target = "testCopyToDeleteMarkerObjectTarget";

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));
		client.deleteObject(d -> d.bucket(bucketName).key(source));

		var e = assertThrows(AwsServiceException.class,
				() -> client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
						.destinationBucket(bucketName).destinationKey(target)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Overwrite")
	public void testObjectVersioningCopyToItselfWithMetadata() {
		var client = getClient();
		var bucketName = createBucket(client);
		var source = "testObjectVersioningCopyToItselfWithMetadataSource";

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));

		var metaData = new HashMap<String, String>();
		metaData.put("foo", "bar");

		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(source)
				.destinationBucket(bucketName).destinationKey(source)
				.metadata(metaData).metadataDirective(MetadataDirective.REPLACE));
		var response = client.getObject(g -> g.bucket(bucketName).key(source));

		assertEquals(metaData, response.response().metadata());

		//버전이 2개인지 확인
		var versionResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(2, versionResponse.versions().size());
	}

	@Test
	@Tag("Overwrite")
	public void testObjectCopyToItselfWithMetadataOverwrite() {
		var client = getClient();
		var bucketName = createBucket(client);
		var source = "testObjectCopyToItselfWithMetadataOverwriteSource";
		var metadata = new HashMap<String, String>();
		metadata.put("foo", "bar");

		client.putObject(p -> p.bucket(bucketName).key(source).metadata(metadata), RequestBody.fromString(source));
		var response = client.headObject(h -> h.bucket(bucketName).key(source));
		assertEquals(metadata, response.metadata());

		metadata.put("foo", "bar2");
		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
				.destinationBucket(bucketName).destinationKey(source)
				.metadata(metadata).metadataDirective(MetadataDirective.REPLACE));
		response = client.headObject(h -> h.bucket(bucketName).key(source));

		assertEquals(metadata, response.metadata());
	}

	@Test
	@Tag("Overwrite")
	public void testObjectVersioningCopyToItselfWithMetadataOverwrite() {
		var client = getClient();
		var bucketName = createBucket(client);
		var source = "testObjectVersioningCopyToItselfWithMetadataOverwriteSource";
		var metadata = new HashMap<String, String>();
		metadata.put("foo", "bar");

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		client.putObject(p -> p.bucket(bucketName).key(source).metadata(metadata), RequestBody.fromString(source));
		var response = client.headObject(h -> h.bucket(bucketName).key(source));
		assertEquals(metadata, response.metadata());

		metadata.put("foo", "bar2");
		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
				.destinationBucket(bucketName).destinationKey(source)
				.metadata(metadata).metadataDirective(MetadataDirective.REPLACE));
		response = client.headObject(h -> h.bucket(bucketName).key(source));

		assertEquals(metadata, response.metadata());
		
		//버전이 2개인지 확인
		var versionResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(2, versionResponse.versions().size());
	}

	@Test
	@Tag("ERROR")
	public void testCopyRevokeSseAlgorithm() {
		var client = getClientHttps(true);
		var bucketName = createBucket(client);
		var sourceKey = "testCopyRevokeSseAlgorithmSource";
		var targetKey = "testCopyRevokeSseAlgorithmTarget";
		var data = Utils.randomTextToLong(1024);

		client.putObject(
				p -> p.bucket(bucketName).key(sourceKey)
						.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM).sseCustomerKey(SSE_KEY)
						.sseCustomerKeyMD5(SSE_KEY_MD5),
				RequestBody.fromString(data));

		var e = assertThrows(AwsServiceException.class, () -> client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(sourceKey).copySourceSSECustomerKey(SSE_KEY)
				.copySourceSSECustomerKeyMD5(SSE_KEY_MD5)
				.destinationBucket(bucketName).destinationKey(targetKey)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
	}

	@Test
	@Tag("checksum")
	public void testCopyObjectChecksumUseChunkEncoding() {
		record TestConfig(
				RequestChecksumCalculation requestOption,
				ResponseChecksumValidation responseOption) {
		}

		var bucketName = createBucket();

		var configs = List.of(
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_REQUIRED),
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_SUPPORTED),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_REQUIRED),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_SUPPORTED));

		var checksums = List.of(
				ChecksumAlgorithm.CRC32,
				ChecksumAlgorithm.CRC32_C,
				ChecksumAlgorithm.CRC64_NVME,
				ChecksumAlgorithm.SHA1,
				ChecksumAlgorithm.SHA256);

		for (var config : configs) {
			var client = getClient(true, config.requestOption, config.responseOption);
			var asyncClient = getAsyncClient(true, config.requestOption, config.responseOption);

			for (var checksum : checksums) {
				var prefix = String.format("req_%s/resp_%s",
						config.requestOption().name(),
						config.responseOption().name());

				// Sync
				var sourceKey = prefix + "/source/sync/" + checksum.name();
				var targetKey = prefix + "/target/sync/" + checksum.name();

				var response = client.putObject(p -> p.bucket(bucketName).key(sourceKey).checksumAlgorithm(checksum),
						RequestBody.fromString(sourceKey));
				checksumCompare(checksum, sourceKey, response);

				var copyResponse = client
						.copyObject(c -> c.sourceBucket(bucketName).sourceKey(sourceKey).destinationBucket(bucketName)
								.destinationKey(targetKey));
				checksumCompare(checksum, sourceKey, copyResponse);

				// Async
				var asyncSourceKey = prefix + "/source/async/" + checksum.name();
				var asyncTargetKey = prefix + "/target/async/" + checksum.name();
				var asyncResponse = asyncClient.putObject(
						p -> p.bucket(bucketName).key(asyncSourceKey).checksumAlgorithm(checksum),
						AsyncRequestBody.fromString(asyncSourceKey));
				checksumCompare(checksum, asyncSourceKey, asyncResponse.join());
				var asyncCopyResponse = asyncClient
						.copyObject(c -> c.sourceBucket(bucketName).sourceKey(asyncSourceKey)
								.destinationBucket(bucketName).destinationKey(asyncTargetKey));
				checksumCompare(checksum, asyncSourceKey, asyncCopyResponse.join());
			}
		}
	}

	@Test
	@Tag("metadata")
	public void testCopyObjectMetadataAndTags() {
		var client = getClient();
		var bucketName = createBucket(client);
		var sourceKey = "testCopyObjectMetadataAndTagsSource";
		var targetKey = "testCopyObjectMetadataAndTagsTarget";

		var metadata = new HashMap<String, String>();
		metadata.put("foo", "bar");

		var tags = List.of(software.amazon.awssdk.services.s3.model.Tag.builder().key("tag1").value("value1").build());
		var tagSet = Tagging.builder().tagSet(tags).build();

		client.putObject(p -> p.bucket(bucketName).key(sourceKey).metadata(metadata).tagging(tagSet),
				RequestBody.fromString(sourceKey));

		var response = client.getObject(g -> g.bucket(bucketName).key(sourceKey));
		assertEquals(metadata, response.response().metadata());

		var tagResponse = client.getObjectTagging(g -> g.bucket(bucketName).key(sourceKey));
		assertEquals(tags, tagResponse.tagSet());

		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(sourceKey).destinationBucket(bucketName)
				.destinationKey(targetKey));

		response = client.getObject(g -> g.bucket(bucketName).key(targetKey));
		assertEquals(metadata, response.response().metadata());

		tagResponse = client.getObjectTagging(g -> g.bucket(bucketName).key(targetKey));
		assertEquals(tags, tagResponse.tagSet());
	}
}
