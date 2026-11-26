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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.CORSConfiguration;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.ObjectAttributes;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Tagging;

@Execution(ExecutionMode.CONCURRENT)
public class BucketOwner extends TestBase {
	@Test
	@Tag("Check")
	public void testBucketOwnerHeadBucket() {
		var client = getClient();
		var bucketName = createBucket(client, 1);

		client.headBucket(h -> h.bucket(bucketName).expectedBucketOwner(getOwner()));
	}

	@Test
	@Tag("ERROR")
	public void testBucketOwnerHeadBucketMismatch() {
		var client = getClient();
		var bucketName = createBucket(client, 2);

		var e = assertThrows(S3Exception.class,
				() -> client.headBucket(h -> h.bucket(bucketName).expectedBucketOwner(getWrongOwner())));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
	}

	@Test
	@Tag("Check")
	public void testBucketOwnerListObjectsV2() {
		var key = "testBucketOwnerListObjectsV2";
		var client = getClient();
		var bucketName = createObjects(client, 3, key);

		var response = client.listObjectsV2(l -> l.bucket(bucketName).expectedBucketOwner(getOwner()));
		assertEquals(1, response.keyCount());
	}

	@Test
	@Tag("ERROR")
	public void testBucketOwnerListObjectsV2Mismatch() {
		var key = "testBucketOwnerListObjectsV2Mismatch";
		var client = getClient();
		var bucketName = createObjects(client, 4, key);

		var e = assertThrows(S3Exception.class,
				() -> client.listObjectsV2(l -> l.bucket(bucketName).expectedBucketOwner(getWrongOwner())));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Check")
	public void testBucketOwnerPutObject() {
		var key = "testBucketOwnerPutObject";
		var client = getClient();
		var bucketName = createBucket(client, 5);

		client.putObject(p -> p.bucket(bucketName).key(key).expectedBucketOwner(getOwner()),
				RequestBody.fromString(key));

		succeedGetObject(client, bucketName, key, key);
	}

	@Test
	@Tag("ERROR")
	public void testBucketOwnerPutObjectMismatch() {
		var key = "testBucketOwnerPutObjectMismatch";
		var client = getClient();
		var bucketName = createBucket(client, 6);

		var e = assertThrows(S3Exception.class,
				() -> client.putObject(p -> p.bucket(bucketName).key(key).expectedBucketOwner(getWrongOwner()),
						RequestBody.fromString(key)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Check")
	public void testBucketOwnerGetObject() {
		var key = "testBucketOwnerGetObject";
		var client = getClient();
		var bucketName = createObjects(client, 7, key);

		var response = client.getObject(g -> g.bucket(bucketName).key(key).expectedBucketOwner(getOwner()));
		assertEquals(key, getBody(response));
	}

	@Test
	@Tag("ERROR")
	public void testBucketOwnerGetObjectMismatch() {
		var key = "testBucketOwnerGetObjectMismatch";
		var client = getClient();
		var bucketName = createObjects(client, 8, key);

		var e = assertThrows(S3Exception.class,
				() -> client.getObject(g -> g.bucket(bucketName).key(key).expectedBucketOwner(getWrongOwner())));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Check")
	public void testBucketOwnerDeleteObject() {
		var key = "testBucketOwnerDeleteObject";
		var client = getClient();
		var bucketName = createObjects(client, 9, key);

		client.deleteObject(d -> d.bucket(bucketName).key(key).expectedBucketOwner(getOwner()));

		failedGetObject(client, bucketName, key, HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY);
	}

	@Test
	@Tag("ERROR")
	public void testBucketOwnerDeleteObjectMismatch() {
		var key = "testBucketOwnerDeleteObjectMismatch";
		var client = getClient();
		var bucketName = createObjects(client, 10, key);

		var e = assertThrows(S3Exception.class,
				() -> client.deleteObject(d -> d.bucket(bucketName).key(key).expectedBucketOwner(getWrongOwner())));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		succeedGetObject(client, bucketName, key, key);
	}

	@Test
	@Tag("Check")
	public void testSourceBucketOwnerCopyObject() {
		var source = "testSourceBucketOwnerCopyObjectSource";
		var target = "testSourceBucketOwnerCopyObjectTarget";
		var client = getClient();
		var sourceBucket = createObjects(client, 11, source);
		var targetBucket = createBucket(client, 11);

		client.copyObject(c -> c
				.sourceBucket(sourceBucket).sourceKey(source)
				.destinationBucket(targetBucket).destinationKey(target)
				.expectedSourceBucketOwner(getOwner())
				.expectedBucketOwner(getOwner()));

		succeedGetObject(client, targetBucket, target, source);
	}

	@Test
	@Tag("ERROR")
	public void testSourceBucketOwnerCopyObjectMismatch() {
		var source = "testSourceBucketOwnerCopyObjectMismatchSource";
		var target = "testSourceBucketOwnerCopyObjectMismatchTarget";
		var client = getClient();
		var sourceBucket = createObjects(client, 12, source);
		var targetBucket = createBucket(client, 12);

		var e = assertThrows(S3Exception.class, () -> client.copyObject(c -> c
				.sourceBucket(sourceBucket).sourceKey(source)
				.destinationBucket(targetBucket).destinationKey(target)
				.expectedSourceBucketOwner(getWrongOwner())
				.expectedBucketOwner(getOwner())));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		failedGetObject(client, targetBucket, target, HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY);
	}

	@Test
	@Tag("ERROR")
	public void testBucketOwnerCopyObjectMismatch() {
		var source = "testBucketOwnerCopyObjectMismatchSource";
		var target = "testBucketOwnerCopyObjectMismatchTarget";
		var client = getClient();
		var sourceBucket = createObjects(client, 13, source);
		var targetBucket = createBucket(client, 13);

		var e = assertThrows(S3Exception.class, () -> client.copyObject(c -> c
				.sourceBucket(sourceBucket).sourceKey(source)
				.destinationBucket(targetBucket).destinationKey(target)
				.expectedSourceBucketOwner(getOwner())
				.expectedBucketOwner(getWrongOwner())));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		failedGetObject(client, targetBucket, target, HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY);
	}

	@Test
	@Tag("Check")
	public void testSourceBucketOwnerUploadPartCopy() {
		var source = "testSourceBucketOwnerUploadPartCopySource";
		var target = "testSourceBucketOwnerUploadPartCopyTarget";
		var client = getClient();
		var sourceBucket = createObjects(client, 14, source);
		var targetBucket = createBucket(client, 14);

		var uploadId = client.createMultipartUpload(c -> c.bucket(targetBucket).key(target)).uploadId();

		var response = client.uploadPartCopy(c -> c
				.sourceBucket(sourceBucket).sourceKey(source)
				.destinationBucket(targetBucket).destinationKey(target)
				.uploadId(uploadId).partNumber(1)
				.expectedSourceBucketOwner(getOwner())
				.expectedBucketOwner(getOwner()));
		assertNotNull(response.copyPartResult().eTag());

		client.abortMultipartUpload(a -> a.bucket(targetBucket).key(target).uploadId(uploadId));
	}

	@Test
	@Tag("ERROR")
	public void testSourceBucketOwnerUploadPartCopyMismatch() {
		var source = "testSourceBucketOwnerUploadPartCopyMismatchSource";
		var target = "testSourceBucketOwnerUploadPartCopyMismatchTarget";
		var client = getClient();
		var sourceBucket = createObjects(client, 15, source);
		var targetBucket = createBucket(client, 15);

		var uploadId = client.createMultipartUpload(c -> c.bucket(targetBucket).key(target)).uploadId();

		var e = assertThrows(S3Exception.class, () -> client.uploadPartCopy(c -> c
				.sourceBucket(sourceBucket).sourceKey(source)
				.destinationBucket(targetBucket).destinationKey(target)
				.uploadId(uploadId).partNumber(1)
				.expectedSourceBucketOwner(getWrongOwner())
				.expectedBucketOwner(getOwner())));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		client.abortMultipartUpload(a -> a.bucket(targetBucket).key(target).uploadId(uploadId));
	}

	@Test
	@Tag("ERROR")
	public void testBucketOwnerUploadPartCopyMismatch() {
		var source = "testBucketOwnerUploadPartCopyMismatchSource";
		var target = "testBucketOwnerUploadPartCopyMismatchTarget";
		var client = getClient();
		var sourceBucket = createObjects(client, 16, source);
		var targetBucket = createBucket(client, 16);

		var uploadId = client.createMultipartUpload(c -> c.bucket(targetBucket).key(target)).uploadId();

		var e = assertThrows(S3Exception.class, () -> client.uploadPartCopy(c -> c
				.sourceBucket(sourceBucket).sourceKey(source)
				.destinationBucket(targetBucket).destinationKey(target)
				.uploadId(uploadId).partNumber(1)
				.expectedSourceBucketOwner(getOwner())
				.expectedBucketOwner(getWrongOwner())));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		client.abortMultipartUpload(a -> a.bucket(targetBucket).key(target).uploadId(uploadId));
	}

	// region 그 밖의 API (AWS 실측 기준)
	// x-amz-expected-bucket-owner 는 버킷·오브젝트를 다루는 거의 모든 API 가 받는다.
	// 소유자가 다르면 다른 검사보다 먼저 403 AccessDenied 다.

	private void assertOwnerMismatch(Executable executable) {
		assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED, executable);
	}

	@Test
	@Tag("List")
	public void testBucketOwnerListOperations() {
		var key = "testBucketOwnerListOperations";
		var client = getClient();
		var bucketName = createObjects(client, 17, key);
		client.createMultipartUpload(c -> c.bucket(bucketName).key("multi"));
		var owner = getOwner();
		var wrong = getWrongOwner();

		assertEquals(1, client.listObjects(l -> l.bucket(bucketName).expectedBucketOwner(owner)).contents().size());
		assertEquals(1,
				client.listObjectVersions(l -> l.bucket(bucketName).expectedBucketOwner(owner)).versions().size());
		assertEquals(1,
				client.listMultipartUploads(l -> l.bucket(bucketName).expectedBucketOwner(owner)).uploads().size());

		assertOwnerMismatch(() -> client.listObjects(l -> l.bucket(bucketName).expectedBucketOwner(wrong)));
		assertOwnerMismatch(() -> client.listObjectVersions(l -> l.bucket(bucketName).expectedBucketOwner(wrong)));
		assertOwnerMismatch(() -> client.listMultipartUploads(l -> l.bucket(bucketName).expectedBucketOwner(wrong)));
	}

	@Test
	@Tag("Bucket")
	public void testBucketOwnerBucketConfig() {
		var client = getClient();
		var bucketName = createBucket(client, 18);
		var owner = getOwner();
		var wrong = getWrongOwner();

		client.getBucketLocation(g -> g.bucket(bucketName).expectedBucketOwner(owner));
		client.getBucketAcl(g -> g.bucket(bucketName).expectedBucketOwner(owner));
		assertOwnerMismatch(() -> client.getBucketLocation(g -> g.bucket(bucketName).expectedBucketOwner(wrong)));
		assertOwnerMismatch(() -> client.getBucketAcl(g -> g.bucket(bucketName).expectedBucketOwner(wrong)));

		// 소유자가 다르면 설정이 바뀌지 않는다
		assertOwnerMismatch(() -> client.putBucketVersioning(p -> p.bucket(bucketName)
				.versioningConfiguration(v -> v.status(BucketVersioningStatus.ENABLED)).expectedBucketOwner(wrong)));
		assertNull(client.getBucketVersioning(g -> g.bucket(bucketName).expectedBucketOwner(owner)).status());
		client.putBucketVersioning(p -> p.bucket(bucketName)
				.versioningConfiguration(v -> v.status(BucketVersioningStatus.ENABLED)).expectedBucketOwner(owner));
		assertOwnerMismatch(() -> client.getBucketVersioning(g -> g.bucket(bucketName).expectedBucketOwner(wrong)));
	}

	@Test
	@Tag("Bucket")
	public void testBucketOwnerBucketTaggingCors() {
		var client = getClient();
		var bucketName = createBucket(client, 19);
		var owner = getOwner();
		var wrong = getWrongOwner();
		var tagging = Tagging.builder().tagSet(t -> t.key("a").value("b")).build();
		var cors = CORSConfiguration.builder().corsRules(r -> r.allowedMethods("GET").allowedOrigins("*")).build();

		assertOwnerMismatch(
				() -> client.putBucketTagging(p -> p.bucket(bucketName).tagging(tagging).expectedBucketOwner(wrong)));
		assertOwnerMismatch(() -> client
				.putBucketCors(p -> p.bucket(bucketName).corsConfiguration(cors).expectedBucketOwner(wrong)));
		client.putBucketTagging(p -> p.bucket(bucketName).tagging(tagging).expectedBucketOwner(owner));
		client.putBucketCors(p -> p.bucket(bucketName).corsConfiguration(cors).expectedBucketOwner(owner));
		assertEquals(tagging.tagSet(),
				client.getBucketTagging(g -> g.bucket(bucketName).expectedBucketOwner(owner)).tagSet());
		assertEquals(1, client.getBucketCors(g -> g.bucket(bucketName).expectedBucketOwner(owner)).corsRules().size());

		assertOwnerMismatch(() -> client.getBucketTagging(g -> g.bucket(bucketName).expectedBucketOwner(wrong)));
		assertOwnerMismatch(() -> client.getBucketCors(g -> g.bucket(bucketName).expectedBucketOwner(wrong)));
		assertOwnerMismatch(() -> client.deleteBucketTagging(d -> d.bucket(bucketName).expectedBucketOwner(wrong)));
		assertOwnerMismatch(() -> client.deleteBucketCors(d -> d.bucket(bucketName).expectedBucketOwner(wrong)));
		// 삭제되지 않았다
		assertEquals(tagging.tagSet(), client.getBucketTagging(g -> g.bucket(bucketName)).tagSet());
		client.deleteBucketTagging(d -> d.bucket(bucketName).expectedBucketOwner(owner));
		client.deleteBucketCors(d -> d.bucket(bucketName).expectedBucketOwner(owner));
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_TAG_SET,
				() -> client.getBucketTagging(g -> g.bucket(bucketName)));
	}

	@Test
	@Tag("Priority")
	public void testBucketOwnerNotConfigured() {
		var client = getClient();
		var bucketName = createBucket(client, 20);
		var owner = getOwner();
		var wrong = getWrongOwner();
		Consumer<String> getPolicy = o -> client.getBucketPolicy(g -> g.bucket(bucketName).expectedBucketOwner(o));
		Consumer<String> getLifecycle = o -> client
				.getBucketLifecycleConfiguration(g -> g.bucket(bucketName).expectedBucketOwner(o));
		Consumer<String> getCors = o -> client.getBucketCors(g -> g.bucket(bucketName).expectedBucketOwner(o));
		Consumer<String> getWebsite = o -> client.getBucketWebsite(g -> g.bucket(bucketName).expectedBucketOwner(o));
		var requests = List.of(
				Map.entry(getPolicy, MainData.NO_SUCH_BUCKET_POLICY),
				Map.entry(getLifecycle, MainData.NO_SUCH_LIFECYCLE_CONFIGURATION),
				Map.entry(getCors, MainData.NO_SUCH_CORS_CONFIGURATION),
				Map.entry(getWebsite, MainData.NO_SUCH_WEBSITE_CONFIGURATION));
		for (var request : requests) {
			// 소유자가 맞으면 설정 없음(404), 다르면 설정을 보기 전에 403 이다
			assertS3Error(HttpStatus.SC_NOT_FOUND, request.getValue(), () -> request.getKey().accept(owner));
			assertOwnerMismatch(() -> request.getKey().accept(wrong));
		}
	}

	@Test
	@Tag("Object")
	public void testBucketOwnerObjectRead() {
		var key = "testBucketOwnerObjectRead";
		var client = getClient();
		var bucketName = createObjects(client, 21, key);
		var owner = getOwner();
		var wrong = getWrongOwner();

		assertEquals(key.length(),
				client.headObject(h -> h.bucket(bucketName).key(key).expectedBucketOwner(owner)).contentLength());
		assertEquals(1,
				client.getObjectAcl(g -> g.bucket(bucketName).key(key).expectedBucketOwner(owner)).grants().size());
		var response = client.getObjectAttributes(g -> g.bucket(bucketName).key(key)
				.objectAttributes(ObjectAttributes.OBJECT_SIZE).expectedBucketOwner(owner));
		assertEquals(key.length(), response.objectSize());

		assertS3Error(HttpStatus.SC_FORBIDDEN, null,
				() -> client.headObject(h -> h.bucket(bucketName).key(key).expectedBucketOwner(wrong)));
		assertOwnerMismatch(
				() -> client.getObjectAcl(g -> g.bucket(bucketName).key(key).expectedBucketOwner(wrong)));
		assertOwnerMismatch(() -> client.getObjectAttributes(g -> g.bucket(bucketName).key(key)
				.objectAttributes(ObjectAttributes.OBJECT_SIZE).expectedBucketOwner(wrong)));
	}

	@Test
	@Tag("Object")
	public void testBucketOwnerObjectTagging() {
		var key = "testBucketOwnerObjectTagging";
		var client = getClient();
		var bucketName = createObjects(client, 22, key);
		var owner = getOwner();
		var wrong = getWrongOwner();
		var tagging = Tagging.builder().tagSet(t -> t.key("a").value("b")).build();

		assertOwnerMismatch(() -> client
				.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(tagging).expectedBucketOwner(wrong)));
		assertEquals(0,
				client.getObjectTagging(g -> g.bucket(bucketName).key(key).expectedBucketOwner(owner)).tagSet().size());
		client.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(tagging).expectedBucketOwner(owner));

		assertOwnerMismatch(
				() -> client.getObjectTagging(g -> g.bucket(bucketName).key(key).expectedBucketOwner(wrong)));
		assertOwnerMismatch(
				() -> client.deleteObjectTagging(d -> d.bucket(bucketName).key(key).expectedBucketOwner(wrong)));
		assertEquals(tagging.tagSet(),
				client.getObjectTagging(g -> g.bucket(bucketName).key(key).expectedBucketOwner(owner)).tagSet());
		client.deleteObjectTagging(d -> d.bucket(bucketName).key(key).expectedBucketOwner(owner));
		assertEquals(0, client.getObjectTagging(g -> g.bucket(bucketName).key(key)).tagSet().size());
	}

	@Test
	@Tag("Priority")
	public void testBucketOwnerKeyNotExist() {
		var key = "testBucketOwnerKeyNotExist";
		var client = getClient();
		var bucketName = createBucket(client, 23);
		// 소유자가 맞으면 없는 키(404), 다르면 키를 보기 전에 403 이다
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_KEY,
				() -> client.getObject(g -> g.bucket(bucketName).key(key).expectedBucketOwner(getOwner())));
		assertOwnerMismatch(
				() -> client.getObject(g -> g.bucket(bucketName).key(key).expectedBucketOwner(getWrongOwner())));
	}

	@Test
	@Tag("Object")
	public void testBucketOwnerDeleteObjects() {
		var client = getClient();
		var bucketName = createBucket(client, 24);
		createObjects(client, bucketName, List.of("a", "b"));
		var delete = Delete.builder().objects(o -> o.key("a"), o -> o.key("b")).build();

		// 요청 전체가 거부되고 하나도 지워지지 않는다
		assertOwnerMismatch(() -> client
				.deleteObjects(d -> d.bucket(bucketName).delete(delete).expectedBucketOwner(getWrongOwner())));
		assertEquals(2, client.listObjectsV2(l -> l.bucket(bucketName)).keyCount());

		var response = client
				.deleteObjects(d -> d.bucket(bucketName).delete(delete).expectedBucketOwner(getOwner()));
		assertEquals(2, response.deleted().size());
		assertEquals(0, client.listObjectsV2(l -> l.bucket(bucketName)).keyCount());
	}

	@Test
	@Tag("Multipart")
	public void testBucketOwnerMultipart() {
		var key = "testBucketOwnerMultipart";
		var client = getClient();
		var bucketName = createBucket(client, 25);
		var owner = getOwner();
		var wrong = getWrongOwner();

		assertOwnerMismatch(
				() -> client.createMultipartUpload(c -> c.bucket(bucketName).key(key).expectedBucketOwner(wrong)));
		var uploadId = client.createMultipartUpload(c -> c.bucket(bucketName).key(key).expectedBucketOwner(owner))
				.uploadId();

		assertOwnerMismatch(() -> client.uploadPart(
				u -> u.bucket(bucketName).key(key).uploadId(uploadId).partNumber(1).expectedBucketOwner(wrong),
				RequestBody.fromString("content")));
		var eTag = client.uploadPart(
				u -> u.bucket(bucketName).key(key).uploadId(uploadId).partNumber(1).expectedBucketOwner(owner),
				RequestBody.fromString("content")).eTag();

		assertOwnerMismatch(() -> client
				.listParts(l -> l.bucket(bucketName).key(key).uploadId(uploadId).expectedBucketOwner(wrong)));
		var response = client
				.listParts(l -> l.bucket(bucketName).key(key).uploadId(uploadId).expectedBucketOwner(owner));
		assertEquals(1, response.parts().size());

		var parts = CompletedMultipartUpload.builder().parts(p -> p.eTag(eTag).partNumber(1)).build();
		assertOwnerMismatch(() -> client.abortMultipartUpload(
				a -> a.bucket(bucketName).key(key).uploadId(uploadId).expectedBucketOwner(wrong)));
		assertOwnerMismatch(() -> client.completeMultipartUpload(c -> c.bucket(bucketName).key(key)
				.uploadId(uploadId).multipartUpload(parts).expectedBucketOwner(wrong)));
		// 거부된 중단·완료는 업로드를 건드리지 않는다
		client.completeMultipartUpload(c -> c.bucket(bucketName).key(key)
				.uploadId(uploadId).multipartUpload(parts).expectedBucketOwner(owner));
		assertEquals("content", getBody(client.getObject(g -> g.bucket(bucketName).key(key))));
	}

	@Test
	@Tag("Bucket")
	public void testBucketOwnerDeleteBucket() {
		var client = getClient();
		var bucketName = createBucket(client, 26);
		assertOwnerMismatch(
				() -> client.deleteBucket(d -> d.bucket(bucketName).expectedBucketOwner(getWrongOwner())));
		client.headBucket(h -> h.bucket(bucketName));

		client.deleteBucket(d -> d.bucket(bucketName).expectedBucketOwner(getOwner()));
		deleteBucketList(bucketName);
		assertS3Error(HttpStatus.SC_NOT_FOUND, null, () -> client.headBucket(h -> h.bucket(bucketName)));
	}

	@Test
	@Tag("Priority")
	public void testBucketOwnerBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(27);
		// 버킷이 없으면 소유자 값과 상관없이 404 다
		for (var owner : List.of(getOwner(), getWrongOwner())) {
			assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET,
					() -> client.listObjectsV2(l -> l.bucket(bucketName).expectedBucketOwner(owner)));
		}
	}

	@Test
	@Tag("Access")
	public void testBucketOwnerOtherUser() {
		var key = "testBucketOwnerOtherUser";
		var client = getClient();
		var bucketName = createObjects(client, 28, key);
		var altClient = getAltClient();
		// 소유자 값이 맞아도 권한이 생기지는 않는다
		for (var owner : List.of(getOwner(), getWrongOwner())) {
			assertOwnerMismatch(() -> altClient.listObjectsV2(l -> l.bucket(bucketName).expectedBucketOwner(owner)));
			assertOwnerMismatch(
					() -> altClient.getObject(g -> g.bucket(bucketName).key(key).expectedBucketOwner(owner)));
		}
	}

	// endregion
}
