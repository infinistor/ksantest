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

import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.Permission;

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
	// 오브젝트의 크기가 0일때 복사가 가능한지 확인
	public void testObjectCopyZeroSize() {
		var key = "foo123bar";
		var newKey = "bar321foo";
		var client = getClient();
		var bucketName = createObjects(client, List.of(key));

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.empty());

		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(key)
				.destinationBucket(bucketName).destinationKey(newKey));

		var response = client.getObject(g -> g.bucket(bucketName).key(newKey));
		assertEquals(0, response.response().contentLength());
	}

	@Test
	@Tag("Check")
	// 동일한 버킷에서 오브젝트 복사가 가능한지 확인
	public void testObjectCopySameBucket() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo123bar";
		var newKey = "bar321foo";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("foo"));

		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(key)
				.destinationBucket(bucketName).destinationKey(newKey));

		var response = client.getObject(g -> g.bucket(bucketName).key(newKey));
		assertEquals("foo", getBody(response));
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

		client.putObject(p -> p.bucket(bucketName).key(key).contentType(contentType),
				RequestBody.fromString("foo"));
		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(key)
				.destinationBucket(bucketName).destinationKey(newKey));

		var response = client.getObject(g -> g.bucket(bucketName).key(newKey));
		assertEquals("foo", getBody(response));
		assertEquals(contentType, response.response().contentType());
	}

	@Test
	@Tag("Overwrite")
	// 복사할 오브젝트와 복사될 오브젝트의 경로가 같을 경우 에러 확인
	public void testObjectCopyToItself() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo123bar";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.empty());

		var e = assertThrows(AwsServiceException.class, () -> client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(key).destinationBucket(bucketName).destinationKey(key)));
		var statusCode = e.statusCode();
		var errorCode = e.awsErrorDetails().errorCode();

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

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("foo"));

		var metaData = new HashMap<String, String>();
		metaData.put("foo", "bar");

		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(key)
				.destinationBucket(bucketName).destinationKey(key)
				.metadataDirective(MetadataDirective.REPLACE)
				.metadata(metaData));
		var response = client.getObject(g -> g.bucket(bucketName).key(key));

		assertEquals(metaData, response.response().metadata());
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

		client.putObject(p -> p.bucket(bucketName1).key(key1),
				RequestBody.fromString("foo"));

		client.copyObject(c -> c
				.sourceBucket(bucketName1).sourceKey(key1)
				.destinationBucket(bucketName2).destinationKey(key2));

		var response = client.getObject(g -> g.bucket(bucketName2).key(key2));
		assertEquals("foo", getBody(response));
	}

	@Test
	@Tag("Check")
	// [bucket1:created main user, object:created main user / bucket2:created sub
	// user] 메인유저가 만든 버킷, 오브젝트를 서브유저가 만든 버킷으로 오브젝트 복사가 불가능한지 확인
	public void testObjectCopyNotOwnedBucket() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName1 = createBucket(client);
		var bucketName2 = createBucket(altClient);

		var key1 = "foo123bar";
		var key2 = "bar321foo";

		client.putObject(p -> p.bucket(bucketName1).key(key1),
				RequestBody.fromString("foo"));

		var e = assertThrows(AwsServiceException.class,
				() -> altClient.copyObject(c -> c
						.sourceBucket(bucketName1).sourceKey(key1)
						.destinationBucket(bucketName2).destinationKey(key2)));
		var statusCode = e.statusCode();

		assertEquals(403, statusCode);
		altClient.deleteBucket(d -> d.bucket(bucketName2));
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

		client.putObject(p -> p.bucket(bucketName).key(key1), RequestBody.fromString("foo"));

		var myGrant = Grant.builder().grantee(config.altUser.toGranteeV2())
				.permission(Permission.FULL_CONTROL).build();
		var grants = addObjectUserGrant(bucketName, key1, myGrant);
		client.putObjectAcl(
				p -> p.bucket(bucketName).key(key1).accessControlPolicy(grants));

		var grants2 = addBucketUserGrant(bucketName, myGrant);
		client.putBucketAcl(p -> p.bucket(bucketName).accessControlPolicy(grants2));

		altClient.getObject(g -> g.bucket(bucketName).key(key1));

		altClient.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(key1)
				.destinationBucket(bucketName).destinationKey(key2));
	}

	@Test
	@Tag("Overwrite")
	// 권한정보를 포함하여 복사할때 올바르게 적용되는지 확인 메타데이터를 포함하여 복사할때 올바르게 적용되는지 확인
	public void testObjectCopyCannedAcl() {
		var key1 = "foo123bar";
		var key2 = "bar321foo";
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucketCannedACL(client);

		client.putObject(p -> p.bucket(bucketName).key(key1),
				RequestBody.fromString("foo"));

		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(key1)
				.destinationBucket(bucketName).destinationKey(key2)
				.acl(ObjectCannedACL.PUBLIC_READ));
		altClient.getObject(g -> g.bucket(bucketName).key(key2));

		var metaData = new HashMap<String, String>();
		metaData.put("abc", "def");

		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(key1)
				.destinationBucket(bucketName).destinationKey(key2)
				.acl(ObjectCannedACL.PUBLIC_READ)
				.metadata(metaData)
				.metadataDirective(MetadataDirective.REPLACE));
		var response = altClient.getObject(g -> g.bucket(bucketName).key(key2));

		assertEquals("foo", getBody(response));
		assertEquals("def", response.response().metadata().get("abc"));
	}

	@Test
	@Tag("Check")
	// 크고 작은 용량의 오브젝트가 복사되는지 확인
	public void testObjectCopyRetainingMetadata() {
		for (var size : new int[] { 3, 1024 * 1024 }) {
			var client = getClient();
			var bucketName = createBucket(client);
			var contentType = "audio/ogg";

			var key1 = "foo123bar";
			var key2 = "bar321foo";

			var metadata = new HashMap<String, String>();
			metadata.put("key1", "value1");
			metadata.put("key2", "value2");

			client.putObject(p -> p.bucket(bucketName).key(key1).metadata(metadata).contentType(contentType),
					RequestBody.fromString(Utils.randomTextToLong(size)));
			client.copyObject(c -> c
					.sourceBucket(bucketName).sourceKey(key1)
					.destinationBucket(bucketName).destinationKey(key2));

			var response = client.getObject(g -> g.bucket(bucketName).key(key2));
			assertEquals(contentType, response.response().contentType());
			assertEquals(metadata, response.response().metadata());
			assertEquals(size, response.response().contentLength());
		}
	}

	@Test
	@Tag("Check")
	// 크고 작은 용량의 오브젝트및 메타데이터가 복사되는지 확인
	public void testObjectCopyReplacingMetadata() {
		for (var size : new int[] { 3, 1024 * 1024 }) {
			var client = getClient();
			var bucketName = createBucket(client);
			var contentType = "audio/ogg";

			var key1 = "foo123bar";
			var key2 = "bar321foo";

			var metadata = new HashMap<String, String>();
			metadata.put("key1", "value1");
			metadata.put("key2", "value2");

			client.putObject(p -> p.bucket(bucketName).key(key1).metadata(metadata).contentType(contentType),
					RequestBody.fromString(Utils.randomTextToLong(size)));

			var response = client.getObject(g -> g.bucket(bucketName).key(key1));
			assertEquals(contentType, response.response().contentType());
			assertEquals(metadata, response.response().metadata());

			var metadata2 = new HashMap<String, String>();
			metadata2.put("key3", "value3");
			metadata2.put("key4", "value4");

			client.copyObject(c -> c
					.sourceBucket(bucketName).sourceKey(key1)
					.destinationBucket(bucketName).destinationKey(key2)
					.metadata(metadata2).contentType(contentType)
					.metadataDirective(MetadataDirective.REPLACE));

			response = client.getObject(g -> g.bucket(bucketName).key(key2));
			assertEquals(contentType, response.response().contentType());
			assertEquals(metadata2, response.response().metadata());
			assertEquals(size, response.response().contentLength());
		}
	}

	@Test
	@Tag("ERROR")
	// 존재하지 않는 버킷에서 존재하지 않는 오브젝트 복사 실패 확인
	public void testObjectCopyBucketNotFound() {
		var client = getClient();
		var bucketName = createBucket(client);
		var e = assertThrows(AwsServiceException.class,
				() -> client.copyObject(c -> c
						.sourceBucket(bucketName).sourceKey("foo123bar")
						.destinationBucket(bucketName + "-fake").destinationKey("bar321foo")));
		var statusCode = e.statusCode();
		assertEquals(404, statusCode);
	}

	@Test
	@Tag("ERROR")
	// 존재하지않는 오브젝트 복사 실패 확인
	public void testObjectCopyKeyNotFound() {
		var client = getClient();
		var bucketName = createBucket(client);

		var e = assertThrows(AwsServiceException.class,
				() -> client.copyObject(c -> c
						.sourceBucket(bucketName).sourceKey("foo123bar")
						.destinationBucket(bucketName).destinationKey("bar321foo")));
		var statusCode = e.statusCode();
		assertEquals(404, statusCode);
	}

	@Test
	@Tag("Version")
	// 버저닝된 오브젝트 복사 확인
	public void testObjectCopyVersionedBucket() {
		var client = getClient();
		var bucketName = createBucket(client);
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		var size = 1 * 5;
		var data = Utils.randomTextToLong(size);
		var key1 = "foo123bar";
		var key2 = "bar321foo";
		var key3 = "bar321foo2";
		client.putObject(p -> p.bucket(bucketName).key(key1), RequestBody.fromString(data));

		var response = client.getObject(g -> g.bucket(bucketName).key(key1));
		var versionId = response.response().versionId();

		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(key1)
				.destinationBucket(bucketName).destinationKey(key2)
				.sourceVersionId(versionId));
		response = client.getObject(g -> g.bucket(bucketName).key(key2));
		assertEquals(data, getBody(response));
		assertEquals(size, response.response().contentLength());

		var versionId2 = response.response().versionId();
		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(key2)
				.destinationBucket(bucketName).destinationKey(key3)
				.sourceVersionId(versionId2));
		response = client.getObject(g -> g.bucket(bucketName).key(key3));
		assertEquals(data, getBody(response));
		assertEquals(size, response.response().contentLength());

		var bucketName2 = createBucket(client);
		checkConfigureVersioningRetry(bucketName2, BucketVersioningStatus.ENABLED);
		var key4 = "bar321foo3";
		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(key1)
				.destinationBucket(bucketName2).destinationKey(key4)
				.sourceVersionId(versionId));
		response = client.getObject(g -> g.bucket(bucketName2).key(key4));
		assertEquals(data, getBody(response));
		assertEquals(size, response.response().contentLength());

		var bucketName3 = createBucket(client);
		checkConfigureVersioningRetry(bucketName3, BucketVersioningStatus.ENABLED);
		var key5 = "bar321foo4";
		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(key1)
				.destinationBucket(bucketName3).destinationKey(key5)
				.sourceVersionId(versionId));
		response = client.getObject(g -> g.bucket(bucketName3).key(key5));
		assertEquals(data, getBody(response));
		assertEquals(size, response.response().contentLength());

		var key6 = "foo123bar2";
		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(key1)
				.destinationBucket(bucketName).destinationKey(key6));
		response = client.getObject(g -> g.bucket(bucketName).key(key6));
		assertEquals(data, getBody(response));
		assertEquals(size, response.response().contentLength());
	}

	@Test
	@Tag("Version")
	// [버킷이 버저닝 가능하고 오브젝트이름에 특수문자가 들어갔을 경우] 오브젝트 복사 성공 확인
	public void testObjectCopyVersionedUrlEncoding() {
		var client = getClient();
		var bucketName = createBucket(client);
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		var sourceKey = "foo?bar";

		client.putObject(p -> p.bucket(bucketName).key(sourceKey),
				RequestBody.fromString("foo"));
		var response = client.getObject(g -> g.bucket(bucketName).key(sourceKey));

		var targetKey = "bar&foo";
		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(sourceKey)
				.destinationBucket(bucketName).destinationKey(targetKey)
				.sourceVersionId(response.response().versionId()));
		client.getObject(g -> g.bucket(bucketName).key(targetKey));
	}

	@Test
	@Tag("Multipart")
	// [버킷에 버저닝 설정] 멀티파트로 업로드된 오브젝트 복사 확인
	public void testObjectCopyVersioningMultipartUpload() {
		var client = getClient();
		var bucketName = createBucket(client);
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		var size = 50 * MainData.MB;
		var key1 = "src_multipart";

		var key1Metadata = new HashMap<String, String>();
		key1Metadata.put("foo", "bar");

		var uploads = setupMultipartUpload(client, bucketName, key1, size, key1Metadata);
		client.completeMultipartUpload(c -> c
				.bucket(bucketName).key(key1).uploadId(uploads.uploadId)
				.multipartUpload(p -> p.parts(uploads.parts)));

		var response = client.headObject(h -> h.bucket(bucketName).key(key1));
		var key1Size = response.contentLength();
		var key1VersionId = response.versionId();

		var key2 = "dst_multipart";
		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(key1)
				.destinationBucket(bucketName).destinationKey(key2)
				.sourceVersionId(key1VersionId));
		response = client.headObject(h -> h.bucket(bucketName).key(key2));
		var key2VersionId = response.versionId();
		assertEquals(key1Size, response.contentLength());
		assertEquals(key1Metadata, response.metadata());
		checkContentUsingRange(bucketName, key2, uploads.getBody(), MainData.MB);

		var key3 = "dst_multipart2";
		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(key2)
				.destinationBucket(bucketName).destinationKey(key3)
				.sourceVersionId(key2VersionId));
		response = client.headObject(h -> h.bucket(bucketName).key(key3));
		assertEquals(key1Size, response.contentLength());
		assertEquals(key1Metadata, response.metadata());
		checkContentUsingRange(bucketName, key3, uploads.getBody(), MainData.MB);

		var bucketName2 = createBucket(client);
		checkConfigureVersioningRetry(bucketName2, BucketVersioningStatus.ENABLED);
		var key4 = "dst_multipart3";
		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(key1)
				.destinationBucket(bucketName2).destinationKey(key4)
				.sourceVersionId(key1VersionId));
		response = client.headObject(h -> h.bucket(bucketName2).key(key4));
		assertEquals(key1Size, response.contentLength());
		assertEquals(key1Metadata, response.metadata());
		checkContentUsingRange(bucketName2, key4, uploads.getBody(), MainData.MB);

		var bucketName3 = createBucket(client);
		checkConfigureVersioningRetry(bucketName3, BucketVersioningStatus.ENABLED);
		var key5 = "dst_multipart4";
		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(key1)
				.destinationBucket(bucketName3).destinationKey(key5)
				.sourceVersionId(key1VersionId));
		response = client.headObject(h -> h.bucket(bucketName3).key(key5));
		assertEquals(key1Size, response.contentLength());
		assertEquals(key1Metadata, response.metadata());
		checkContentUsingRange(bucketName3, key5, uploads.getBody(), MainData.MB);

		var key6 = "dst_multipart5";
		client.copyObject(c -> c
				.sourceBucket(bucketName3).sourceKey(key5)
				.destinationBucket(bucketName).destinationKey(key6));
		response = client.headObject(h -> h.bucket(bucketName).key(key6));
		assertEquals(key1Size, response.contentLength());
		assertEquals(key1Metadata, response.metadata());
		checkContentUsingRange(bucketName, key6, uploads.getBody(), MainData.MB);
	}

	@Test
	@Tag("If Match")
	// if match 값을 추가하여 오브젝트를 복사할 경우 성공확인
	public void testCopyObjectIfMatchGood() {
		var client = getClient();
		var bucketName = createBucket(client);

		var putResponse = client.putObject(p -> p.bucket(bucketName).key("foo"),
				RequestBody.fromString("bar"));

		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey("foo")
				.destinationBucket(bucketName).destinationKey("bar")
				.sourceVersionId(putResponse.versionId())
				.copySourceIfMatch(putResponse.eTag()));
		var getResponse = client.getObject(g -> g.bucket(bucketName).key("bar"));
		assertEquals("bar", getBody(getResponse));
	}

	@Test
	@Tag("If Match")
	// if match에 잘못된 값을 입력하여 오브젝트를 복사할 경우 실패 확인
	public void testCopyObjectIfMatchFailed() {
		var client = getClient();
		var bucketName = createBucket(client);

		var putResponse = client.putObject(p -> p.bucket(bucketName).key("foo"),
				RequestBody.fromString("bar"));

		var e = assertThrows(AwsServiceException.class, () -> client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey("foo")
				.destinationBucket(bucketName).destinationKey("bar")
				.sourceVersionId(putResponse.versionId())
				.copySourceIfMatch("ABC")));
		var statusCode = e.statusCode();
		assertEquals(412, statusCode);

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

		client.putObject(p -> p.bucket(bucketName).key(key1),
				RequestBody.fromString("foo"));
		client.deleteObject(d -> d.bucket(bucketName).key(key1));

		var e = assertThrows(AwsServiceException.class, () -> client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(key1)
				.destinationBucket(bucketName).destinationKey(ker2)));
		var statusCode = e.statusCode();
		var errorCode = e.awsErrorDetails().errorCode();
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

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		client.putObject(p -> p.bucket(bucketName).key(key1),
				RequestBody.fromString("foo"));
		client.deleteObject(d -> d.bucket(bucketName).key(key1));

		var e = assertThrows(AwsServiceException.class, () -> client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(key1)
				.destinationBucket(bucketName).destinationKey(ker2)));
		var statusCode = e.statusCode();
		var errorCode = e.awsErrorDetails().errorCode();
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

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("foo"));

		var metaData = new HashMap<String, String>();
		metaData.put("foo", "bar");

		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(key)
				.destinationBucket(bucketName).destinationKey(key)
				.metadata(metaData)
				.metadataDirective(MetadataDirective.REPLACE));
		var response = client.getObject(g -> g.bucket(bucketName).key(key));

		assertEquals(metaData, response.response().metadata());
	}

	@Test
	@Tag("Overwrite")
	// 복사할 오브젝트와 복사될 오브젝트의 경로가 같지만 메타데이터를 덮어쓰기 모드로 변경하면 해당 오브젝트의 메타데이터가 업데이트되는지 확인
	public void testObjectCopyToItselfWithMetadataOverwrite() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo123bar";
		var metadata = new HashMap<String, String>();
		metadata.put("foo", "bar");

		client.putObject(p -> p.bucket(bucketName).key(key).metadata(metadata), RequestBody.fromString("foo"));
		var response = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(metadata, response.metadata());

		metadata.put("foo", "bar2");
		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(key)
				.destinationBucket(bucketName).destinationKey(key)
				.metadata(metadata)
				.metadataDirective(MetadataDirective.REPLACE));
		response = client.headObject(h -> h.bucket(bucketName).key(key));

		assertEquals(metadata, response.metadata());
	}

	@Test
	@Tag("Overwrite")
	// 복사할 오브젝트와 복사될 오브젝트의 경로가 같지만 메타데이터를 덮어쓰기 모드로 변경하면 해당 오브젝트의 메타데이터가 업데이트되는지
	// 확인(Versioning 설정)
	public void testObjectVersioningCopyToItselfWithMetadataOverwrite() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo123bar";
		var metadata = new HashMap<String, String>();
		metadata.put("foo", "bar");

		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		client.putObject(p -> p.bucket(bucketName).key(key).metadata(metadata), RequestBody.fromString("foo"));
		var response = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals(metadata, response.metadata());

		metadata.put("foo", "bar2");
		client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(key)
				.destinationBucket(bucketName).destinationKey(key)
				.metadata(metadata)
				.metadataDirective(MetadataDirective.REPLACE));
		response = client.headObject(h -> h.bucket(bucketName).key(key));

		assertEquals(metadata, response.metadata());
	}

	@Test
	@Tag("ERROR")
	// sse-c로 암호화된 오브젝트를 복사할때 Algorithm을 누락하면 오류가 발생하는지 확인
	public void testCopyRevokeSseAlgorithm() {
		var client = getClientHttps(true);
		var bucketName = createBucket(client);
		var sourceKey = "sourceKey";
		var targetKey = "targetKey";
		var data = Utils.randomTextToLong(1024);

		client.putObject(
				p -> p.bucket(bucketName).key(sourceKey)
						.sseCustomerAlgorithm(SSE_CUSTOMER_ALGORITHM).sseCustomerKey(SSE_KEY).sseCustomerKeyMD5(SSE_KEY_MD5),
				RequestBody.fromString(data));

		var e = assertThrows(AwsServiceException.class, () -> client.copyObject(c -> c
				.sourceBucket(bucketName).sourceKey(sourceKey).copySourceSSECustomerKey(SSE_KEY).copySourceSSECustomerKeyMD5(SSE_KEY_MD5)
				.destinationBucket(bucketName).destinationKey(targetKey)));
		var statusCode = e.statusCode();
		assertEquals(400, statusCode);
	}
}
