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

import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.CheckSum;
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
		var bucketName = createObjects(client, 1, source);

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
		var bucketName = createBucket(client, 2);
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
		var bucketName = createBucket(client, 3);
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
		var bucketName = createBucket(client, 4);
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
		var bucketName = createBucket(client, 5);
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
		var sourceBucket = createBucket(client, 6);
		var targetBucket = createBucket(client, 6);
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
		var sourceBucket = createBucket(client, 7);
		var targetBucket = createBucket(altClient, 7);
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
		var bucketName = createBucketCannedAcl(client, 8);
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
		var bucketName = createBucketCannedAcl(client, 9);
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
			var bucketName = createBucket(client, 10);
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
			var bucketName = createBucket(client, 11);
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
		var bucketName = createBucket(client, 12);
		var e = assertThrows(AwsServiceException.class,
				() -> client.copyObject(
						c -> c.sourceBucket(bucketName + "-fake").sourceKey("testObjectCopyBucketNotFoundSource")
								.destinationBucket(bucketName).destinationKey("testObjectCopyBucketNotFoundTarget")));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testObjectCopyKeyNotFound() {
		var client = getClient();
		var bucketName = createBucket(client, 13);

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
		var bucketName = createBucket(client, 14);
		var size = 1 * 5;
		var data = Utils.randomTextToLong(size);
		var source = "testObjectCopyVersionedBucketSource";
		var target = "testObjectCopyVersionedBucketTarget";

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		var putResponse = client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(data));
		var sourceVid = putResponse.versionId();

		var copyResponse = client.copyObject(c -> c
				.sourceBucket(bucketName)
				.sourceKey(source)
				.sourceVersionId(sourceVid)
				.destinationBucket(bucketName)
				.destinationKey(target));
		var targetVid = copyResponse.versionId();

		var response = client.getObject(g -> g.bucket(bucketName).key(target));

		assertEquals(data, getBody(response));
		assertEquals(size, response.response().contentLength());

		var target2 = "testObjectCopyVersionedBucketTarget2";
		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(target).sourceVersionId(targetVid)
				.destinationBucket(bucketName).destinationKey(target2));

		response = client.getObject(g -> g.bucket(bucketName).key(target2));
		assertEquals(data, getBody(response));
		assertEquals(size, response.response().contentLength());

		var targetBucket = createBucket(client, 14);
		checkConfigureVersioningRetry(targetBucket, BucketVersioningStatus.ENABLED);
		var target3 = "testObjectCopyVersionedBucketTarget3";

		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source).sourceVersionId(sourceVid)
				.destinationBucket(targetBucket).destinationKey(target3));

		response = client.getObject(g -> g.bucket(targetBucket).key(target3));
		assertEquals(data, getBody(response));
		assertEquals(size, response.response().contentLength());

		var bucketName3 = createBucket(client, 14);
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
		var bucketName = createBucket(client, 15);
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
		var bucketName = createBucket(client, 16);
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

		var targetBucket = createBucket(client, 16);
		checkConfigureVersioningRetry(targetBucket, BucketVersioningStatus.ENABLED);

		var target3 = "testObjectCopyVersioningMultipartUploadTarget3";
		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source).sourceVersionId(sourceVid)
				.destinationBucket(targetBucket).destinationKey(target3));
		response = client.headObject(h -> h.bucket(targetBucket).key(target3));
		assertEquals(sourceSize, response.contentLength());
		assertEquals(sourceMetadata, response.metadata());
		checkContentUsingRange(targetBucket, target3, uploads.getBody(), MainData.MB);

		var bucketName3 = createBucket(client, 16);
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
		var bucketName = createBucket(client, 17);
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
		var bucketName = createBucket(client, 18);
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
	@Tag("If Match")
	// 소스 오브젝트와 일치하지 않는 copy-source-if-none-match 조건으로 복사 성공 확인
	public void testCopyObjectIfNoneMatchGood() {
		var client = getClient();
		var bucketName = createBucket(client, 19);
		var source = "testCopyObjectIfNoneMatchGoodSource";
		var target = "testCopyObjectIfNoneMatchGoodTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));

		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source).copySourceIfNoneMatch("ABC")
				.destinationBucket(bucketName).destinationKey(target));
		var response = client.getObject(g -> g.bucket(bucketName).key(target));
		assertEquals(source, getBody(response));
	}

	@Test
	@Tag("If Match")
	// 소스 오브젝트와 일치하는 copy-source-if-none-match 조건으로 복사 시 412 실패 확인
	public void testCopyObjectIfNoneMatchFailed() {
		var client = getClient();
		var bucketName = createBucket(client, 20);
		var source = "testCopyObjectIfNoneMatchFailedSource";
		var target = "testCopyObjectIfNoneMatchFailedTarget";

		var eTag = client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source)).eTag();

		var e = assertThrows(AwsServiceException.class,
				() -> client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
						.destinationBucket(bucketName).destinationKey(target).copySourceIfNoneMatch(eTag)));

		assertEquals(HttpStatus.SC_PRECONDITION_FAILED, e.statusCode());
		assertEquals(MainData.PRECONDITION_FAILED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("If Match")
	// 소스 오브젝트 업로드 이전 시간의 copy-source-if-modified-since 조건으로 복사 성공 확인
	public void testCopyObjectIfModifiedSinceGood() {
		var client = getClient();
		var bucketName = createBucket(client, 21);
		var source = "testCopyObjectIfModifiedSinceGoodSource";
		var target = "testCopyObjectIfModifiedSinceGoodTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));

		var days = Calendar.getInstance();
		days.set(1994, 8, 29, 19, 43, 31);

		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
				.copySourceIfModifiedSince(days.toInstant())
				.destinationBucket(bucketName).destinationKey(target));
		var response = client.getObject(g -> g.bucket(bucketName).key(target));
		assertEquals(source, getBody(response));
	}

	@Test
	@Tag("If Match")
	// 소스 오브젝트 업로드 이후 시간의 copy-source-if-modified-since 조건으로 복사 시 412 실패 확인
	public void testCopyObjectIfModifiedSinceFailed() {
		var client = getClient();
		var bucketName = createBucket(client, 22);
		var source = "testCopyObjectIfModifiedSinceFailedSource";
		var target = "testCopyObjectIfModifiedSinceFailedTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));

		// 미래 날짜는 RFC 7232에 따라 무시되므로 업로드 시간 + 1초를 지정하고 1초 대기
		var lastModified = client.headObject(h -> h.bucket(bucketName).key(source)).lastModified();
		var after = lastModified.plus(1, ChronoUnit.SECONDS);

		delay(1000);

		var e = assertThrows(AwsServiceException.class,
				() -> client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
						.copySourceIfModifiedSince(after)
						.destinationBucket(bucketName).destinationKey(target)));

		assertEquals(HttpStatus.SC_PRECONDITION_FAILED, e.statusCode());
		assertEquals(MainData.PRECONDITION_FAILED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("If Match")
	// 소스 오브젝트 업로드 이후 시간의 copy-source-if-unmodified-since 조건으로 복사 성공 확인
	public void testCopyObjectIfUnmodifiedSinceGood() {
		var client = getClient();
		var bucketName = createBucket(client, 23);
		var source = "testCopyObjectIfUnmodifiedSinceGoodSource";
		var target = "testCopyObjectIfUnmodifiedSinceGoodTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));

		var days = Calendar.getInstance();
		days.set(2100, 8, 29, 19, 43, 31);

		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
				.copySourceIfUnmodifiedSince(days.toInstant())
				.destinationBucket(bucketName).destinationKey(target));
		var response = client.getObject(g -> g.bucket(bucketName).key(target));
		assertEquals(source, getBody(response));
	}

	@Test
	@Tag("If Match")
	// 소스 오브젝트 업로드 이전 시간의 copy-source-if-unmodified-since 조건으로 복사 시 412 실패 확인
	public void testCopyObjectIfUnmodifiedSinceFailed() {
		var client = getClient();
		var bucketName = createBucket(client, 24);
		var source = "testCopyObjectIfUnmodifiedSinceFailedSource";
		var target = "testCopyObjectIfUnmodifiedSinceFailedTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));

		var days = Calendar.getInstance();
		days.set(1994, 8, 29, 19, 43, 31);

		var e = assertThrows(AwsServiceException.class,
				() -> client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
						.copySourceIfUnmodifiedSince(days.toInstant())
						.destinationBucket(bucketName).destinationKey(target)));

		assertEquals(HttpStatus.SC_PRECONDITION_FAILED, e.statusCode());
		assertEquals(MainData.PRECONDITION_FAILED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("If Match")
	// copy-source-if-match(일치)와 copy-source-if-unmodified-since(불일치)를 함께 사용할 경우
	// ETag 조건이 우선되어 복사에 성공하는지 확인
	public void testCopyObjectIfMatchWithIfUnmodifiedSince() {
		var client = getClient();
		var bucketName = createBucket(client, 25);
		var source = "testCopyObjectIfMatchWithIfUnmodifiedSinceSource";
		var target = "testCopyObjectIfMatchWithIfUnmodifiedSinceTarget";

		var eTag = client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source)).eTag();

		var days = Calendar.getInstance();
		days.set(1994, 8, 29, 19, 43, 31);

		// copy-source-if-match: true, copy-source-if-unmodified-since: false -> 200 OK
		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
				.copySourceIfMatch(eTag).copySourceIfUnmodifiedSince(days.toInstant())
				.destinationBucket(bucketName).destinationKey(target));
		var response = client.getObject(g -> g.bucket(bucketName).key(target));
		assertEquals(source, getBody(response));
	}

	@Test
	@Tag("If Match")
	// copy-source-if-none-match(불일치)와 copy-source-if-modified-since(일치)를 함께 사용할 경우
	// ETag 조건이 우선되어 412가 반환되는지 확인
	public void testCopyObjectIfNoneMatchWithIfModifiedSince() {
		var client = getClient();
		var bucketName = createBucket(client, 26);
		var source = "testCopyObjectIfNoneMatchWithIfModifiedSinceSource";
		var target = "testCopyObjectIfNoneMatchWithIfModifiedSinceTarget";

		var eTag = client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source)).eTag();

		var days = Calendar.getInstance();
		days.set(1994, 8, 29, 19, 43, 31);

		// copy-source-if-none-match: false, copy-source-if-modified-since: true -> 412
		var e = assertThrows(AwsServiceException.class,
				() -> client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
						.copySourceIfNoneMatch(eTag).copySourceIfModifiedSince(days.toInstant())
						.destinationBucket(bucketName).destinationKey(target)));

		assertEquals(HttpStatus.SC_PRECONDITION_FAILED, e.statusCode());
		assertEquals(MainData.PRECONDITION_FAILED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("IfMatch")
	@Tag("IfNoneMatch")
	// copy-source-if-match와 copy-source-if-none-match에 동일한 ETag를 지정하면 412가 반환되는지 확인
	public void testCopyObjectIfMatchAndIfNoneMatch() {
		var client = getClient();
		var bucketName = createBucket(client, 27);
		var source = "testCopyObjectIfMatchAndIfNoneMatchSource";
		var target = "testCopyObjectIfMatchAndIfNoneMatchTarget";

		var eTag = client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source)).eTag();

		var e = assertThrows(AwsServiceException.class,
				() -> client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
						.copySourceIfMatch(eTag).copySourceIfNoneMatch(eTag)
						.destinationBucket(bucketName).destinationKey(target)));
		assertEquals(HttpStatus.SC_PRECONDITION_FAILED, e.statusCode());
		assertEquals(MainData.PRECONDITION_FAILED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("IfMatch")
	@Tag("IfNoneMatch")
	// copy-source-if-match와 copy-source-if-none-match: * 를 함께 지정하면 412가 반환되는지 확인
	public void testCopyObjectIfMatchAndIfNoneMatchAny() {
		var client = getClient();
		var bucketName = createBucket(client, 28);
		var source = "testCopyObjectIfMatchAndIfNoneMatchAnySource";
		var target = "testCopyObjectIfMatchAndIfNoneMatchAnyTarget";

		var eTag = client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source)).eTag();

		var e = assertThrows(AwsServiceException.class,
				() -> client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
						.copySourceIfMatch(eTag).copySourceIfNoneMatch("*")
						.destinationBucket(bucketName).destinationKey(target)));
		assertEquals(HttpStatus.SC_PRECONDITION_FAILED, e.statusCode());
		assertEquals(MainData.PRECONDITION_FAILED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("IfMatch")
	// 대상 오브젝트와 일치하는 If-Match 조건으로 덮어쓰기 복사 성공 확인
	public void testCopyObjectDestinationIfMatchGood() {
		var client = getClient();
		var bucketName = createBucket(client, 29);
		var source = "testCopyObjectDestinationIfMatchGoodSource";
		var target = "testCopyObjectDestinationIfMatchGoodTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));
		var targetETag = client.putObject(p -> p.bucket(bucketName).key(target), RequestBody.fromString("old"))
				.eTag();

		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
				.destinationBucket(bucketName).destinationKey(target).ifMatch(targetETag));
		var response = client.getObject(g -> g.bucket(bucketName).key(target));
		assertEquals(source, getBody(response));
	}

	@Test
	@Tag("IfMatch")
	// 대상 오브젝트와 일치하지 않는 If-Match 조건으로 덮어쓰기 복사 시 412 실패 확인
	public void testCopyObjectDestinationIfMatchFailed() {
		var client = getClient();
		var bucketName = createBucket(client, 30);
		var source = "testCopyObjectDestinationIfMatchFailedSource";
		var target = "testCopyObjectDestinationIfMatchFailedTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));
		client.putObject(p -> p.bucket(bucketName).key(target), RequestBody.fromString("old"));

		var e = assertThrows(AwsServiceException.class,
				() -> client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
						.destinationBucket(bucketName).destinationKey(target)
						.ifMatch("ABCDEFGHIJKLMNOPQRSTUVWXYZ")));

		assertEquals(HttpStatus.SC_PRECONDITION_FAILED, e.statusCode());
		assertEquals(MainData.PRECONDITION_FAILED, e.awsErrorDetails().errorCode());

		// 덮어쓰기 되지 않았는지 확인
		var response = client.getObject(g -> g.bucket(bucketName).key(target));
		assertEquals("old", getBody(response));
	}

	@Test
	@Tag("IfNoneMatch")
	// 존재하지 않는 대상 키에 If-None-Match: * 조건으로 복사 성공 확인
	public void testCopyObjectDestinationIfNoneMatchGood() {
		var client = getClient();
		var bucketName = createBucket(client, 31);
		var source = "testCopyObjectDestinationIfNoneMatchGoodSource";
		var target = "testCopyObjectDestinationIfNoneMatchGoodTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));

		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
				.destinationBucket(bucketName).destinationKey(target).ifNoneMatch("*"));
		var response = client.getObject(g -> g.bucket(bucketName).key(target));
		assertEquals(source, getBody(response));
	}

	@Test
	@Tag("IfNoneMatch")
	// 이미 존재하는 대상 키에 If-None-Match: * 조건으로 복사 시 412 실패 확인
	public void testCopyObjectDestinationIfNoneMatchFailed() {
		var client = getClient();
		var bucketName = createBucket(client, 32);
		var source = "testCopyObjectDestinationIfNoneMatchFailedSource";
		var target = "testCopyObjectDestinationIfNoneMatchFailedTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));
		client.putObject(p -> p.bucket(bucketName).key(target), RequestBody.fromString("old"));

		var e = assertThrows(AwsServiceException.class,
				() -> client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
						.destinationBucket(bucketName).destinationKey(target).ifNoneMatch("*")));

		assertEquals(HttpStatus.SC_PRECONDITION_FAILED, e.statusCode());
		assertEquals(MainData.PRECONDITION_FAILED, e.awsErrorDetails().errorCode());

		// 덮어쓰기 되지 않았는지 확인
		var response = client.getObject(g -> g.bucket(bucketName).key(target));
		assertEquals("old", getBody(response));
	}

	@Test
	@Tag("IfMatch")
	@Tag("IfNoneMatch")
	// 대상에 If-Match와 If-None-Match를 함께 지정하면 501로 거부되는지 확인
	public void testCopyObjectDestinationIfMatchAndIfNoneMatch() {
		var client = getClient();
		var bucketName = createBucket(client, 33);
		var source = "testCopyObjectDestinationIfMatchAndIfNoneMatchSource";
		var target = "testCopyObjectDestinationIfMatchAndIfNoneMatchTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));
		var targetETag = client.putObject(p -> p.bucket(bucketName).key(target), RequestBody.fromString("old"))
				.eTag();

		var e = assertThrows(AwsServiceException.class,
				() -> client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
						.destinationBucket(bucketName).destinationKey(target)
						.ifMatch(targetETag).ifNoneMatch(targetETag)));
		assertEquals(HttpStatus.SC_NOT_IMPLEMENTED, e.statusCode());
		assertEquals(MainData.NOT_IMPLEMENTED, e.awsErrorDetails().errorCode());

		// 덮어쓰기 되지 않았는지 확인
		var response = client.getObject(g -> g.bucket(bucketName).key(target));
		assertEquals("old", getBody(response));
	}

	@Test
	@Tag("IfMatch")
	@Tag("IfNoneMatch")
	// 대상에 If-Match와 If-None-Match: * 를 함께 지정하면 501로 거부되는지 확인
	public void testCopyObjectDestinationIfMatchAndIfNoneMatchAny() {
		var client = getClient();
		var bucketName = createBucket(client, 34);
		var source = "testCopyObjectDestinationIfMatchAndIfNoneMatchAnySource";
		var target = "testCopyObjectDestinationIfMatchAndIfNoneMatchAnyTarget";

		client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source));
		var targetETag = client.putObject(p -> p.bucket(bucketName).key(target), RequestBody.fromString("old"))
				.eTag();

		var e = assertThrows(AwsServiceException.class,
				() -> client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source)
						.destinationBucket(bucketName).destinationKey(target)
						.ifMatch(targetETag).ifNoneMatch("*")));
		assertEquals(HttpStatus.SC_NOT_IMPLEMENTED, e.statusCode());
		assertEquals(MainData.NOT_IMPLEMENTED, e.awsErrorDetails().errorCode());

		// 덮어쓰기 되지 않았는지 확인
		var response = client.getObject(g -> g.bucket(bucketName).key(target));
		assertEquals("old", getBody(response));
	}

	@Test
	@Tag("IfMatch")
	@Tag("IfNoneMatch")
	// 소스 If-Match와 대상 If-None-Match: * 를 함께 사용해 복사 성공 확인
	public void testCopyObjectSourceIfMatchWithDestinationIfNoneMatch() {
		var client = getClient();
		var bucketName = createBucket(client, 35);
		var source = "testCopyObjectSourceIfMatchWithDestinationIfNoneMatchSource";
		var target = "testCopyObjectSourceIfMatchWithDestinationIfNoneMatchTarget";

		var eTag = client.putObject(p -> p.bucket(bucketName).key(source), RequestBody.fromString(source)).eTag();

		client.copyObject(c -> c.sourceBucket(bucketName).sourceKey(source).copySourceIfMatch(eTag)
				.destinationBucket(bucketName).destinationKey(target).ifNoneMatch("*"));
		var response = client.getObject(g -> g.bucket(bucketName).key(target));
		assertEquals(source, getBody(response));
	}

	@Test
	@Tag("encryption")
	public void testCopyNorSrcToNorBucketAndObj() {
		var prefix = "testCopyNorSrcToNorBucketAndObj";
		testObjectCopy(36, prefix, false, false, false, false, 1024);
		testObjectCopy(36, prefix, false, false, false, false, 256 * 1024);
		testObjectCopy(36, prefix, false, false, false, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyNorSrcToNorBucketEncryptionObj() {
		var prefix = "testCopyNorSrcToNorBucketEncryptionObj";
		testObjectCopy(37, prefix, false, false, false, true, 1024);
		testObjectCopy(37, prefix, false, false, false, true, 256 * 1024);
		testObjectCopy(37, prefix, false, false, false, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyNorSrcToEncryptionBucketNorObj() {
		var prefix = "testCopyNorSrcToEncryptionBucketNorObj";
		testObjectCopy(38, prefix, false, false, true, false, 1024);
		testObjectCopy(38, prefix, false, false, true, false, 256 * 1024);
		testObjectCopy(38, prefix, false, false, true, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyNorSrcToEncryptionBucketAndObj() {
		var prefix = "testCopyNorSrcToEncryptionBucketAndObj";
		testObjectCopy(39, prefix, false, false, true, true, 1024);
		testObjectCopy(39, prefix, false, false, true, true, 256 * 1024);
		testObjectCopy(39, prefix, false, false, true, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionSrcToNorBucketAndObj() {
		var prefix = "testCopyEncryptionSrcToNorBucketAndObj";
		testObjectCopy(40, prefix, true, false, false, false, 1024);
		testObjectCopy(40, prefix, true, false, false, false, 256 * 1024);
		testObjectCopy(40, prefix, true, false, false, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionSrcToNorBucketEncryptionObj() {
		var prefix = "testCopyEncryptionSrcToNorBucketEncryptionObj";
		testObjectCopy(41, prefix, true, false, false, true, 1024);
		testObjectCopy(41, prefix, true, false, false, true, 256 * 1024);
		testObjectCopy(41, prefix, true, false, false, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionSrcToEncryptionBucketNorObj() {
		var prefix = "testCopyEncryptionSrcToEncryptionBucketNorObj";
		testObjectCopy(42, prefix, true, false, true, false, 1024);
		testObjectCopy(42, prefix, true, false, true, false, 256 * 1024);
		testObjectCopy(42, prefix, true, false, true, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionSrcToEncryptionBucketAndObj() {
		var prefix = "testCopyEncryptionSrcToEncryptionBucketAndObj";
		testObjectCopy(43, prefix, true, false, true, true, 1024);
		testObjectCopy(43, prefix, true, false, true, true, 256 * 1024);
		testObjectCopy(43, prefix, true, false, true, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionBucketNorObjToNorBucketAndObj() {
		var prefix = "testCopyEncryptionBucketNorObjToNorBucketAndObj";
		testObjectCopy(44, prefix, false, true, false, false, 1024);
		testObjectCopy(44, prefix, false, true, false, false, 256 * 1024);
		testObjectCopy(44, prefix, false, true, false, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionBucketNorObjToNorBucketEncryptionObj() {
		var prefix = "testCopyEncryptionBucketNorObjToNorBucketEncryptionObj";
		testObjectCopy(45, prefix, false, true, false, true, 1024);
		testObjectCopy(45, prefix, false, true, false, true, 256 * 1024);
		testObjectCopy(45, prefix, false, true, false, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionBucketNorObjToEncryptionBucketNorObj() {
		var prefix = "testCopyEncryptionBucketNorObjToEncryptionBucketNorObj";
		testObjectCopy(46, prefix, false, true, true, false, 1024);
		testObjectCopy(46, prefix, false, true, true, false, 256 * 1024);
		testObjectCopy(46, prefix, false, true, true, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionBucketNorObjToEncryptionBucketAndObj() {
		var prefix = "testCopyEncryptionBucketNorObjToEncryptionBucketAndObj";
		testObjectCopy(47, prefix, false, true, true, true, 1024);
		testObjectCopy(47, prefix, false, true, true, true, 256 * 1024);
		testObjectCopy(47, prefix, false, true, true, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionBucketAndObjToNorBucketAndObj() {
		var prefix = "testCopyEncryptionBucketAndObjToNorBucketAndObj";
		testObjectCopy(48, prefix, true, true, false, false, 1024);
		testObjectCopy(48, prefix, true, true, false, false, 256 * 1024);
		testObjectCopy(48, prefix, true, true, false, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionBucketAndObjToNorBucketEncryptionObj() {
		var prefix = "testCopyEncryptionBucketAndObjToNorBucketEncryptionObj";
		testObjectCopy(49, prefix, true, true, false, true, 1024);
		testObjectCopy(49, prefix, true, true, false, true, 256 * 1024);
		testObjectCopy(49, prefix, true, true, false, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionBucketAndObjToEncryptionBucketNorObj() {
		var prefix = "testCopyEncryptionBucketAndObjToEncryptionBucketNorObj";
		testObjectCopy(50, prefix, true, true, true, false, 1024);
		testObjectCopy(50, prefix, true, true, true, false, 256 * 1024);
		testObjectCopy(50, prefix, true, true, true, false, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyEncryptionBucketAndObjToEncryptionBucketAndObj() {
		var prefix = "testCopyEncryptionBucketAndObjToEncryptionBucketAndObj";
		testObjectCopy(51, prefix, true, true, true, true, 1024);
		testObjectCopy(51, prefix, true, true, true, true, 256 * 1024);
		testObjectCopy(51, prefix, true, true, true, true, 1024 * 1024);
	}

	@Test
	@Tag("encryption")
	public void testCopyToNormalSource() {
		var prefix = "testCopyToNormalSource";
		var size1 = 1024;
		var size2 = 256 * 1024;
		var size3 = 1024 * 1024;

		testObjectCopy(52, prefix, EncryptionType.NORMAL, EncryptionType.NORMAL, size1);
		testObjectCopy(52, prefix, EncryptionType.NORMAL, EncryptionType.NORMAL, size2);
		testObjectCopy(52, prefix, EncryptionType.NORMAL, EncryptionType.NORMAL, size3);

		testObjectCopy(52, prefix, EncryptionType.NORMAL, EncryptionType.SSE_S3, size1);
		testObjectCopy(52, prefix, EncryptionType.NORMAL, EncryptionType.SSE_S3, size2);
		testObjectCopy(52, prefix, EncryptionType.NORMAL, EncryptionType.SSE_S3, size3);

		testObjectCopy(52, prefix, EncryptionType.NORMAL, EncryptionType.SSE_C, size1);
		testObjectCopy(52, prefix, EncryptionType.NORMAL, EncryptionType.SSE_C, size2);
		testObjectCopy(52, prefix, EncryptionType.NORMAL, EncryptionType.SSE_C, size3);
	}

	@Test
	@Tag("encryption")
	public void testCopyToSseS3Source() {
		var prefix = "testCopyToSseS3Source";
		var size1 = 1024;
		var size2 = 256 * 1024;
		var size3 = 1024 * 1024;

		testObjectCopy(53, prefix, EncryptionType.SSE_S3, EncryptionType.NORMAL, size1);
		testObjectCopy(53, prefix, EncryptionType.SSE_S3, EncryptionType.NORMAL, size2);
		testObjectCopy(53, prefix, EncryptionType.SSE_S3, EncryptionType.NORMAL, size3);

		testObjectCopy(53, prefix, EncryptionType.SSE_S3, EncryptionType.SSE_S3, size1);
		testObjectCopy(53, prefix, EncryptionType.SSE_S3, EncryptionType.SSE_S3, size2);
		testObjectCopy(53, prefix, EncryptionType.SSE_S3, EncryptionType.SSE_S3, size3);

		testObjectCopy(53, prefix, EncryptionType.SSE_S3, EncryptionType.SSE_C, size1);
		testObjectCopy(53, prefix, EncryptionType.SSE_S3, EncryptionType.SSE_C, size2);
		testObjectCopy(53, prefix, EncryptionType.SSE_S3, EncryptionType.SSE_C, size3);
	}

	@Test
	@Tag("encryption")
	public void testCopyToSseCSource() {
		var prefix = "testCopyToSseCSource";
		var size1 = 1024;
		var size2 = 256 * 1024;
		var size3 = 1024 * 1024;

		testObjectCopy(54, prefix, EncryptionType.SSE_C, EncryptionType.NORMAL, size1);
		testObjectCopy(54, prefix, EncryptionType.SSE_C, EncryptionType.NORMAL, size2);
		testObjectCopy(54, prefix, EncryptionType.SSE_C, EncryptionType.NORMAL, size3);

		testObjectCopy(54, prefix, EncryptionType.SSE_C, EncryptionType.SSE_S3, size1);
		testObjectCopy(54, prefix, EncryptionType.SSE_C, EncryptionType.SSE_S3, size2);
		testObjectCopy(54, prefix, EncryptionType.SSE_C, EncryptionType.SSE_S3, size3);

		testObjectCopy(54, prefix, EncryptionType.SSE_C, EncryptionType.SSE_C, size1);
		testObjectCopy(54, prefix, EncryptionType.SSE_C, EncryptionType.SSE_C, size2);
		testObjectCopy(54, prefix, EncryptionType.SSE_C, EncryptionType.SSE_C, size3);
	}

	@Test
	@Tag("ERROR")
	public void testCopyToDeletedObject() {
		var client = getClient();
		var bucketName = createBucket(client, 55);
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
		var bucketName = createBucket(client, 56);
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
		var bucketName = createBucket(client, 57);
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

		// 버전이 2개인지 확인
		var versionResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(2, versionResponse.versions().size());
	}

	@Test
	@Tag("Overwrite")
	public void testObjectCopyToItselfWithMetadataOverwrite() {
		var client = getClient();
		var bucketName = createBucket(client, 58);
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
		var bucketName = createBucket(client, 59);
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

		// 버전이 2개인지 확인
		var versionResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(2, versionResponse.versions().size());
	}

	@Test
	@Tag("ERROR")
	public void testCopyRevokeSseAlgorithm() {
		var client = getClientHttps(true);
		var bucketName = createBucket(client, 60);
		unblockSseC(bucketName);
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

		var bucketName = createBucket(61);

		var configs = List.of(
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_REQUIRED),
				new TestConfig(RequestChecksumCalculation.WHEN_REQUIRED,
						ResponseChecksumValidation.WHEN_SUPPORTED),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_REQUIRED),
				new TestConfig(RequestChecksumCalculation.WHEN_SUPPORTED,
						ResponseChecksumValidation.WHEN_SUPPORTED));

		// CopyObject는 모든 체크섬 알고리즘 사용 가능
		var checksums = CheckSum.ALL_ALGORITHMS;

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

				var response = client.putObject(
						p -> CheckSum.applyChecksum(p.bucket(bucketName).key(sourceKey), checksum, sourceKey),
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
						p -> CheckSum.applyChecksum(p.bucket(bucketName).key(asyncSourceKey), checksum,
								asyncSourceKey),
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
		var bucketName = createBucket(client, 62);
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
