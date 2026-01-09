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

import java.time.Instant;
import java.util.HashMap;

import org.example.Data.BackendHeaders;
import org.example.Data.MainData;
import org.example.Data.MultipartUploadV2Data;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectLockRetentionMode;
import software.amazon.awssdk.services.s3.model.Permission;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;

public class Backend extends TestBase {

	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Backend V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Backend V2 End");
	}

	/**
	 * Backend 헤더를 사용하여 오브젝트를 업로드할 수 있는지 확인
	 */
	public void testPutObject() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var key = "testPutObject";
		var content = "test content";

		// Backend 클라이언트로 업로드
		var response = backendClient.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));
		assertEquals(200, response.sdkHttpResponse().statusCode());

		// 일반 클라이언트로 다운로드하여 확인
		var getResponse = client.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(getResponse);
		assertEquals(content, body);
	}

	/**
	 * Backend 헤더를 사용하여 오브젝트를 다운로드할 수 있는지 확인
	 */
	public void testGetObject() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var key = "testGetObject";
		var content = "test content";

		// 일반 클라이언트로 업로드
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));

		// Backend 클라이언트로 다운로드
		var response = backendClient.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(response);
		assertEquals(content, body);
	}

	/**
	 * Backend 헤더를 사용하여 오브젝트를 삭제할 수 있는지 확인
	 */
	public void testDeleteObject() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var key = "testDeleteObject";
		var content = "test content";

		// 일반 클라이언트로 업로드
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));

		// Backend 클라이언트로 삭제
		var response = backendClient.deleteObject(d -> d.bucket(bucketName).key(key));
		assertEquals(204, response.sdkHttpResponse().statusCode());

		// 일반 클라이언트로 오브젝트 목록 확인
		var listResponse = client.listObjectsV2(l -> l.bucket(bucketName));
		assertEquals(0, listResponse.contents().size());
	}

	/**
	 * Backend 헤더를 사용하여 오브젝트를 복사할 수 있는지 확인
	 */
	public void testCopyObject() {
		var client = getClient();
		var backendClient = getBackendClient();
		var sourceBucket = createBucket(client);
		var targetBucket = createBucket(client);
		var sourceKey = "sourceKey";
		var targetKey = "targetKey";
		var content = "test content";

		// 일반 클라이언트로 소스 오브젝트 업로드
		client.putObject(p -> p.bucket(sourceBucket).key(sourceKey), RequestBody.fromString(content));

		// Backend 클라이언트로 복사
		var response = backendClient.copyObject(c -> c
				.sourceBucket(sourceBucket)
				.sourceKey(sourceKey)
				.destinationBucket(targetBucket)
				.destinationKey(targetKey));
		assertEquals(200, response.sdkHttpResponse().statusCode());

		// 일반 클라이언트로 타겟 오브젝트 확인
		var getResponse = client.getObject(g -> g.bucket(targetBucket).key(targetKey));
		var body = getBody(getResponse);
		assertEquals(content, body);
	}

	/**
	 * Backend 헤더를 사용하여 멀티파트 업로드를 수행할 수 있는지 확인
	 */
	public void testMultipartUpload() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var key = "testMultipartUpload";
		var size = 10 * MainData.MB;

		// Backend 클라이언트로 멀티파트 업로드
		var uploadData = setupMultipartUpload(backendClient, bucketName, key, size, DEFAULT_PART_SIZE);
		var completeResponse = backendClient
				.completeMultipartUpload(c -> c.bucket(bucketName).key(key).uploadId(uploadData.uploadId)
						.multipartUpload(p -> p.parts(uploadData.parts)));
		var versionId = completeResponse.versionId();

		// 일반 클라이언트로 확인
		var response = client.headObject(h -> h.bucket(bucketName).key(key).versionId(versionId));
		assertEquals(size, response.contentLength());

		checkContentUsingRange(bucketName, key, uploadData.getBody(), MainData.MB);
	}

	/**
	 * Backend 헤더를 사용하여 오브젝트 ACL을 설정할 수 있는지 확인
	 */
	public void testPutObjectAcl() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucketCannedAcl(client);
		var key = "testPutObjectAcl";
		var content = "test content";

		// 일반 클라이언트로 업로드
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));

		// Backend 클라이언트로 ACL 설정
		var response = backendClient.putObjectAcl(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ));
		assertEquals(200, response.sdkHttpResponse().statusCode());

		// 일반 클라이언트로 ACL 확인
		var aclResponse = client.getObjectAcl(g -> g.bucket(bucketName).key(key));
		assertEquals(2, aclResponse.grants().size());
	}

	/**
	 * Backend 헤더를 사용하여 오브젝트 ACL을 조회할 수 있는지 확인
	 */
	public void testGetObjectAcl() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucketCannedAcl(client);
		var key = "testGetObjectAcl";
		var content = "test content";

		// 일반 클라이언트로 업로드 및 ACL 설정
		client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ),
				RequestBody.fromString(content));

		// Backend 클라이언트로 ACL 조회
		var response = backendClient.getObjectAcl(g -> g.bucket(bucketName).key(key));
		assertEquals(2, response.grants().size());
	}

	/**
	 * Backend 헤더를 사용하여 오브젝트 태그를 설정할 수 있는지 확인
	 */
	public void testPutObjectTagging() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var key = "testPutObjectTagging";
		var content = "test content";

		// 일반 클라이언트로 업로드
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));

		// Backend 클라이언트로 태그 설정
		var tag = Tag.builder().key("testKey").value("testValue").build();
		var tagging = Tagging.builder().tagSet(tag).build();
		var response = backendClient.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(tagging));
		assertEquals(200, response.sdkHttpResponse().statusCode());

		// 일반 클라이언트로 태그 확인
		var getResponse = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		assertEquals(1, getResponse.tagSet().size());
		assertEquals("testKey", getResponse.tagSet().get(0).key());
		assertEquals("testValue", getResponse.tagSet().get(0).value());
	}

	/**
	 * Backend 헤더를 사용하여 오브젝트 태그를 조회할 수 있는지 확인
	 */
	public void testGetObjectTagging() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var key = "testGetObjectTagging";
		var content = "test content";
		var tag = Tag.builder().key("testKey").value("testValue").build();

		// 일반 클라이언트로 업로드 및 태그 설정
		var tagging = Tagging.builder().tagSet(tag).build();
		client.putObject(p -> p.bucket(bucketName).key(key).tagging(tagging), RequestBody.fromString(content));

		// Backend 클라이언트로 태그 조회
		var response = backendClient.getObjectTagging(g -> g.bucket(bucketName).key(key));
		assertEquals(1, response.tagSet().size());
		assertEquals("testKey", response.tagSet().get(0).key());
		assertEquals("testValue", response.tagSet().get(0).value());
	}

	/**
	 * Backend 헤더를 사용하여 오브젝트 태그를 삭제할 수 있는지 확인
	 */
	public void testDeleteObjectTagging() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var key = "testDeleteObjectTagging";
		var content = "test content";
		var tag = Tag.builder().key("testKey").value("testValue").build();

		// 일반 클라이언트로 업로드 및 태그 설정
		var tagging = Tagging.builder().tagSet(tag).build();
		client.putObject(p -> p.bucket(bucketName).key(key).tagging(tagging), RequestBody.fromString(content));

		// Backend 클라이언트로 태그 삭제
		var response = backendClient.deleteObjectTagging(d -> d.bucket(bucketName).key(key));
		assertEquals(204, response.sdkHttpResponse().statusCode());

		// 일반 클라이언트로 태그 확인
		var getResponse = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		assertEquals(0, getResponse.tagSet().size());
	}

	/**
	 * Backend 헤더를 사용하여 오브젝트 보존 설정을 할 수 있는지 확인
	 */
	public void testPutObjectRetention() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var key = "testPutObjectRetention";
		var content = "test content";

		// Object Lock이 활성화된 버킷이 필요하므로 테스트 제한
		// 일반 클라이언트로 업로드
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));

		// Backend 클라이언트로 보존 설정
		var retainUntilDate = getExpiredDate(Instant.now(), 1);
		var response = backendClient.putObjectRetention(p -> p.bucket(bucketName).key(key)
				.retention(r -> r.mode(ObjectLockRetentionMode.COMPLIANCE).retainUntilDate(retainUntilDate))
				.bypassGovernanceRetention(true));
		assertEquals(200, response.sdkHttpResponse().statusCode());
	}

	/**
	 * Backend 헤더를 사용하여 오브젝트 보존 설정을 조회할 수 있는지 확인
	 */
	public void testGetObjectRetention() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var key = "testGetObjectRetention";
		var content = "test content";

		// 일반 클라이언트로 업로드
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));

		// Backend 클라이언트로 보존 설정 조회
		// Object Lock이 활성화되지 않은 경우 예외 발생
		assertThrows(AwsServiceException.class,
				() -> backendClient.getObjectRetention(g -> g.bucket(bucketName).key(key)));
	}

	/**
	 * Backend 헤더를 사용하여 오브젝트 보존 설정을 삭제할 수 있는지 확인
	 */
	public void testDeleteObjectRetention() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var key = "testDeleteObjectRetention";
		var content = "test content";

		// 일반 클라이언트로 업로드
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));

		// Backend 클라이언트로 보존 설정 삭제
		// Object Lock이 활성화되지 않은 경우 예외 발생
		assertThrows(AwsServiceException.class,
				() -> backendClient.putObjectRetention(p -> p.bucket(bucketName).key(key)
						.retention(r -> r.mode(ObjectLockRetentionMode.COMPLIANCE))
						.bypassGovernanceRetention(true)));
	}

	/**
	 * [Versioning] PutObject가 정상 동작하는지 확인
	 */
	public void testPutObjectVersioning() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var key = "testPutObjectVersioning";
		var content = "test content";

		// 버저닝 활성화
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		// Backend 클라이언트로 업로드
		var response = backendClient.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));
		assertEquals(200, response.sdkHttpResponse().statusCode());

		// 일반 클라이언트로 다운로드하여 확인
		var getResponse = client.getObject(g -> g.bucket(bucketName).key(key));
		var body = getBody(getResponse);
		assertEquals(content, body);
	}

	/**
	 * [Versioning] PutObject 버전 정보 추가시 정상 동작 확인
	 */
	public void testPutObjectVersioningWithVersionId() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var key = "testPutObjectVersioningWithVersionIdSource";
		var key2 = "testPutObjectVersioningWithVersionIdTarget";
		var content = "test content";
		var content2 = "test content2";

		// 버저닝 활성화
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 업로드
		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));
		var versionId = putResponse.versionId();

		// Backend 클라이언트로 업로드
		var response = backendClient.putObject(
				p -> p.bucket(bucketName).key(key2)
						.overrideConfiguration(o -> o
								.putHeader(BackendHeaders.IFS_VERSION_ID, versionId)
								.putHeader(BackendHeaders.KSAN_VERSION_ID, versionId)),
				RequestBody.fromString(content2));
		assertEquals(200, response.sdkHttpResponse().statusCode());

		// 일반 클라이언트로 다운로드하여 버전 정보가 일치하는지 확인
		var getResponse = client.getObject(g -> g.bucket(bucketName).key(key2).versionId(versionId));
		var body = getBody(getResponse);
		assertEquals(content2, body);
		assertEquals(versionId, getResponse.response().versionId());
	}

	/**
	 * [Versioning] GetObject가 정상 동작하는지 확인
	 */
	public void testGetObjectVersioning() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var key = "testGetObjectVersioning";
		var content = "test content";

		// 버저닝 활성화
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 업로드
		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));
		var versionId = putResponse.versionId();

		// Backend 클라이언트로 다운로드
		var response = backendClient.getObject(g -> g.bucket(bucketName).key(key).versionId(versionId));
		var body = getBody(response);
		assertEquals(content, body);
	}

	/**
	 * [Versioning] DeleteObject가 정상 동작하는지 확인
	 */
	public void testDeleteObjectVersioning() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var key = "testDeleteObjectVersioning";
		var content = "test content";

		// 버저닝 활성화
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 업로드
		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));
		var versionId = putResponse.versionId();

		// Backend 클라이언트로 삭제 (삭제 마커 생성)
		var response = backendClient.deleteObject(d -> d.bucket(bucketName).key(key));
		assertEquals(204, response.sdkHttpResponse().statusCode());

		// 일반 클라이언트로 버전 목록 확인 (삭제 마커가 생성됨)
		var listResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(1, listResponse.deleteMarkers().size());

		// Backend 클라이언트로 버전 포함하여 삭제
		var deleteResponse = backendClient.deleteObject(d -> d.bucket(bucketName).key(key).versionId(versionId));
		assertEquals(204, deleteResponse.sdkHttpResponse().statusCode());

		// 일반 클라이언트로 버전 목록 확인
		listResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(0, listResponse.versions().size());
		assertEquals(1, listResponse.deleteMarkers().size());
	}

	/**
	 * [Versioning] HeadObject가 정상 동작하는지 확인
	 */
	public void testHeadObjectVersioning() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var key = "testHeadObjectVersioning";
		var content = "test content";

		// 버저닝 활성화
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 업로드
		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));
		var versionId = putResponse.versionId();

		// Backend 클라이언트로 헤더 조회
		var response = backendClient.headObject(h -> h.bucket(bucketName).key(key).versionId(versionId));
		assertEquals(200, response.sdkHttpResponse().statusCode());
		assertEquals(content.length(), response.contentLength());
		assertEquals(versionId, response.versionId());
	}

	/**
	 * [Versioning] CopyObject가 정상 동작하는지 확인
	 */
	public void testCopyObjectVersioning() {
		var client = getClient();
		var backendClient = getBackendClient();
		var sourceBucket = createBucket(client);
		var targetBucket = createBucket(client);
		var sourceKey = "sourceKey";
		var sourceKey2 = "sourceKey2";
		var targetKey = "targetKey";
		var content = "test content";

		// 소스 버킷에 버저닝 활성화
		checkConfigureVersioningRetry(sourceBucket, BucketVersioningStatus.ENABLED);
		// 타겟 버킷에 버저닝 활성화
		checkConfigureVersioningRetry(targetBucket, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 소스 오브젝트 업로드 및 복사
		var putResponse = client.putObject(p -> p.bucket(sourceBucket).key(sourceKey), RequestBody.fromString(content));
		var sourceVid = putResponse.versionId();

		var copyResponse = client.copyObject(c -> c
				.sourceBucket(sourceBucket)
				.sourceKey(sourceKey)
				.sourceVersionId(sourceVid)
				.destinationBucket(sourceBucket)
				.destinationKey(sourceKey2));
		var targetVid = copyResponse.versionId();

		// Backend 클라이언트로 복사
		backendClient.copyObject(c -> c
				.sourceBucket(sourceBucket)
				.sourceKey(sourceKey2)
				.destinationBucket(targetBucket)
				.destinationKey(targetKey)
				.sourceVersionId(targetVid)
				.overrideConfiguration(o -> o
						.putHeader(BackendHeaders.IFS_VERSION_ID, targetVid)
						.putHeader(BackendHeaders.KSAN_VERSION_ID, targetVid)));

		// 일반 클라이언트로 타겟 오브젝트 확인
		var getResponse = client.getObject(g -> g.bucket(targetBucket).key(targetKey));
		var body = getBody(getResponse);
		assertEquals(content, body);
		assertEquals(targetVid, getResponse.response().versionId());
	}

	/**
	 * [Versioning] MultipartUpload가 정상 동작하는지 확인
	 */
	public void testMultipartUploadVersioning() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var key = "testMultipartUploadVersioning";
		var size = 10 * MainData.MB;

		// 버저닝 활성화
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		// Backend 클라이언트로 멀티파트 업로드
		var uploadData = setupMultipartUpload(backendClient, bucketName, key, size, DEFAULT_PART_SIZE);
		var completeResponse = backendClient
				.completeMultipartUpload(c -> c.bucket(bucketName).key(key).uploadId(uploadData.uploadId)
						.multipartUpload(p -> p.parts(uploadData.parts)));
		var versionId = completeResponse.versionId();

		// 일반 클라이언트로 확인
		var response = client.headObject(h -> h.bucket(bucketName).key(key).versionId(versionId));
		assertEquals(size, response.contentLength());

		checkContentUsingRange(bucketName, key, uploadData.getBody(), MainData.MB);
	}

	/**
	 * [Versioning] PutObjectAcl가 정상 동작하는지 확인
	 */
	public void testPutObjectAclVersioning() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucketCannedAcl(client);
		var key = "testPutObjectAclVersioning";
		var content = "test content";

		// 버저닝 활성화
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 업로드
		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));
		var versionId = putResponse.versionId();

		// Backend 클라이언트로 ACL 설정
		var response = backendClient
				.putObjectAcl(p -> p.bucket(bucketName).key(key).versionId(versionId).acl(ObjectCannedACL.PUBLIC_READ));
		assertEquals(200, response.sdkHttpResponse().statusCode());

		// 일반 클라이언트로 ACL 확인
		var aclResponse = client.getObjectAcl(g -> g.bucket(bucketName).key(key).versionId(versionId));
		assertEquals(2, aclResponse.grants().size());
	}

	/**
	 * [Versioning] GetObjectAcl가 정상 동작하는지 확인
	 */
	public void testGetObjectAclVersioning() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucketCannedAcl(client);
		var key = "testGetObjectAclVersioning";
		var content = "test content";

		// 버저닝 활성화
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 업로드 및 ACL 설정
		client.putObject(p -> p.bucket(bucketName).key(key).acl(ObjectCannedACL.PUBLIC_READ),
				RequestBody.fromString(content));

		// Backend 클라이언트로 ACL 조회
		var response = backendClient.getObjectAcl(g -> g.bucket(bucketName).key(key));
		assertEquals(2, response.grants().size());
	}

	/**
	 * [Versioning] PutObjectTagging가 정상 동작하는지 확인
	 */
	public void testPutObjectTaggingVersioning() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var key = "testPutObjectTaggingVersioning";
		var content = "test content";

		// 버저닝 활성화
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 업로드
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));

		// Backend 클라이언트로 태그 설정
		var tag = Tag.builder().key("testKey").value("testValue").build();
		var tagging = Tagging.builder().tagSet(tag).build();
		var response = backendClient.putObjectTagging(p -> p.bucket(bucketName).key(key).tagging(tagging));
		assertEquals(200, response.sdkHttpResponse().statusCode());

		// 일반 클라이언트로 태그 확인
		var getResponse = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		assertEquals(1, getResponse.tagSet().size());
		assertEquals("testKey", getResponse.tagSet().get(0).key());
		assertEquals("testValue", getResponse.tagSet().get(0).value());
	}

	/**
	 * [Versioning] GetObjectTagging가 정상 동작하는지 확인
	 */
	public void testGetObjectTaggingVersioning() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var key = "testGetObjectTaggingVersioning";
		var content = "test content";
		var tag = Tag.builder().key("testKey").value("testValue").build();

		// 버저닝 활성화
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 업로드 및 태그 설정
		var tagging = Tagging.builder().tagSet(tag).build();
		client.putObject(p -> p.bucket(bucketName).key(key).tagging(tagging), RequestBody.fromString(content));

		// Backend 클라이언트로 태그 조회
		var response = backendClient.getObjectTagging(g -> g.bucket(bucketName).key(key));
		assertEquals(1, response.tagSet().size());
		assertEquals("testKey", response.tagSet().get(0).key());
		assertEquals("testValue", response.tagSet().get(0).value());
	}

	/**
	 * [Versioning] DeleteObjectTagging가 정상 동작하는지 확인
	 */
	public void testDeleteObjectTaggingVersioning() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var key = "testDeleteObjectTaggingVersioning";
		var content = "test content";
		var tag = Tag.builder().key("testKey").value("testValue").build();

		// 버저닝 활성화
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 업로드 및 태그 설정
		var tagging = Tagging.builder().tagSet(tag).build();
		client.putObject(p -> p.bucket(bucketName).key(key).tagging(tagging), RequestBody.fromString(content));

		// Backend 클라이언트로 태그 삭제
		var response = backendClient.deleteObjectTagging(d -> d.bucket(bucketName).key(key));
		assertEquals(204, response.sdkHttpResponse().statusCode());

		// 일반 클라이언트로 태그 확인
		var getResponse = client.getObjectTagging(g -> g.bucket(bucketName).key(key));
		assertEquals(0, getResponse.tagSet().size());
	}

	/**
	 * [Versioning] PutObjectRetention가 정상 동작하는지 확인
	 */
	public void testPutObjectRetentionVersioning() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = getNewBucketName();
		var key = "testPutObjectRetentionVersioning";
		var content = "test content";

		// 버킷 생성
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		// 버저닝 활성화
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 업로드
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));

		// Backend 클라이언트로 보존 설정
		var retainUntilDate = getExpiredDate(Instant.now(), 1);
		var response = backendClient.putObjectRetention(p -> p.bucket(bucketName).key(key)
				.retention(r -> r.mode(ObjectLockRetentionMode.COMPLIANCE).retainUntilDate(retainUntilDate))
				.bypassGovernanceRetention(true));
		assertEquals(200, response.sdkHttpResponse().statusCode());
	}

	/**
	 * [Versioning] GetObjectRetention가 정상 동작하는지 확인
	 */
	public void testGetObjectRetentionVersioning() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = getNewBucketName();
		var key = "testGetObjectRetentionVersioning";
		var content = "test content";

		// 버킷 생성
		client.createBucket(c -> c.bucket(bucketName).objectLockEnabledForBucket(true));

		// 버저닝 활성화
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 업로드
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));

		// Backend 클라이언트로 보존 설정 조회
		// Object Lock이 활성화되지 않은 경우 예외 발생
		assertThrows(AwsServiceException.class,
				() -> backendClient.getObjectRetention(g -> g.bucket(bucketName).key(key)));
	}

	/**
	 * PutObject 복제가 정상 동작하는지 확인
	 */
	public void testPutObjectReplication() {
		var client = getClient();
		var backendClient = getBackendClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var key = "testBackendReplication";
		var content = "test content";

		// 버저닝 활성화
		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningStatus.ENABLED);
		checkConfigureVersioningRetry(targetBucketName, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 업로드
		var putResponse = client.putObject(p -> p.bucket(sourceBucketName).key(key), RequestBody.fromString(content));
		var versionId = putResponse.versionId();

		// Backend 클라이언트로 복사
		backendPutObject(backendClient, sourceBucketName, key, targetBucketName, key, versionId);

		// 일반 클라이언트로 확인
		var getResponse = client.getObject(g -> g.bucket(targetBucketName).key(key));
		var body = getBody(getResponse);
		assertEquals(content, body);
		assertEquals(versionId, getResponse.response().versionId());
	}

	/**
	 * PutObject 태그가 복제되는지 확인
	 */
	public void testPutObjectWithTaggingReplication() {
		var client = getClient();
		var backendClient = getBackendClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var key = "testBackendReplicationTagging";
		var content = "test content";
		var tagging = Tagging.builder().tagSet(Tag.builder().key("testKey").value("testValue").build()).build();

		// 버저닝 활성화
		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningStatus.ENABLED);
		checkConfigureVersioningRetry(targetBucketName, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 업로드
		var putResponse = client.putObject(p -> p.bucket(sourceBucketName).key(key).tagging(tagging),
				RequestBody.fromString(content));
		var versionId = putResponse.versionId();

		// Backend 클라이언트로 복사
		backendPutObject(backendClient, sourceBucketName, key, targetBucketName, key, versionId);

		// 일반 클라이언트로 확인
		var getResponse = client.getObject(g -> g.bucket(targetBucketName).key(key));
		var body = getBody(getResponse);
		assertEquals(content, body);
		assertEquals(versionId, getResponse.response().versionId());

		var tagResponse = client.getObjectTagging(g -> g.bucket(targetBucketName).key(key));
		tagCompare(tagging.tagSet(), tagResponse.tagSet());
	}

	/**
	 * PutObject 헤더와 메타데이터가 복제되는지 확인
	 */
	public void testPutObjectWithMetadataReplication() {
		var client = getClient();
		var backendClient = getBackendClient();
		var sourceBucketName = createBucket(client);
		var targetBucketName = createBucket(client);
		var key = "testBackendReplicationMetadata";
		var content = "test content";
		var metadata = new HashMap<String, String>();
		metadata.put("testKey", "testValue");

		// 버저닝 활성화
		checkConfigureVersioningRetry(sourceBucketName, BucketVersioningStatus.ENABLED);
		checkConfigureVersioningRetry(targetBucketName, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 업로드
		var putResponse = client.putObject(p -> p.bucket(sourceBucketName).key(key).metadata(metadata),
				RequestBody.fromString(content));
		var versionId = putResponse.versionId();

		// Backend 클라이언트로 복사
		backendPutObject(backendClient, sourceBucketName, key, targetBucketName, key, versionId);

		// 일반 클라이언트로 확인
		var getResponse = client.getObject(g -> g.bucket(targetBucketName).key(key));
		var body = getBody(getResponse);
		assertEquals(content, body);
		assertEquals(versionId, getResponse.response().versionId());
		assertEquals(metadata, getResponse.response().metadata());
	}

	/**
	 * CopyObject 복제가 정상 동작하는지 확인
	 */
	public void testCopyObjectReplication() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucket = createBucket(client);
		var sourceKey = "sourceKey";
		var sourceKey2 = "sourceKey2";
		var targetKey = "targetKey";
		var content = "test content";

		// 버킷에 버저닝 활성화
		checkConfigureVersioningRetry(bucket, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 소스 오브젝트 업로드 및 복사
		var putResponse = client.putObject(p -> p.bucket(bucket).key(sourceKey), RequestBody.fromString(content));
		var sourceVid = putResponse.versionId();

		var copyResponse = client.copyObject(c -> c
				.sourceBucket(bucket)
				.sourceKey(sourceKey)
				.sourceVersionId(sourceVid)
				.destinationBucket(bucket)
				.destinationKey(sourceKey2));
		var targetVid = copyResponse.versionId();

		// Backend 클라이언트로 복사
		backendCopyObject(backendClient, bucket, sourceKey2, bucket, targetKey, targetVid, targetVid);

		// 일반 클라이언트로 타겟 오브젝트 확인
		var getResponse = client.getObject(g -> g.bucket(bucket).key(targetKey));
		var body = getBody(getResponse);
		assertEquals(content, body);
		assertEquals(targetVid, getResponse.response().versionId());
	}

	/**
	 * CopyObject 태그가 복제되는지 확인
	 */
	public void testCopyObjectWithTaggingReplication() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucket = createBucket(client);
		var sourceKey = "sourceKey";
		var sourceKey2 = "sourceKey2";
		var targetKey = "targetKey";
		var content = "test content";
		var tagging = Tagging.builder().tagSet(Tag.builder().key("testKey").value("testValue").build()).build();

		// 버킷에 버저닝 활성화
		checkConfigureVersioningRetry(bucket, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 소스 오브젝트 업로드 및 복사
		var putResponse = client.putObject(p -> p.bucket(bucket).key(sourceKey).tagging(tagging),
				RequestBody.fromString(content));
		var sourceVid = putResponse.versionId();

		var copyResponse = client.copyObject(c -> c
				.sourceBucket(bucket)
				.sourceKey(sourceKey)
				.sourceVersionId(sourceVid)
				.destinationBucket(bucket)
				.destinationKey(sourceKey2));
		var targetVid = copyResponse.versionId();

		// Backend 클라이언트로 복사
		backendCopyObject(backendClient, bucket, sourceKey2, bucket, targetKey, targetVid, targetVid);

		// 일반 클라이언트로 타겟 오브젝트 확인
		var getResponse = client.getObject(g -> g.bucket(bucket).key(targetKey));
		var body = getBody(getResponse);
		assertEquals(content, body);
		assertEquals(targetVid, getResponse.response().versionId());

		var tagResponse = client.getObjectTagging(g -> g.bucket(bucket).key(targetKey));
		tagCompare(tagging.tagSet(), tagResponse.tagSet());
	}

	/**
	 * CopyObject 헤더와 메타데이터가 복제되는지 확인
	 */
	public void testCopyObjectWithMetadataReplication() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucket = createBucket(client);
		var sourceKey = "sourceKey";
		var sourceKey2 = "sourceKey2";
		var targetKey = "targetKey";
		var content = "test content";
		var metadata = new HashMap<String, String>();
		metadata.put("testKey", "testValue");

		// 버킷에 버저닝 활성화
		checkConfigureVersioningRetry(bucket, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 소스 오브젝트 업로드 및 복사
		var putResponse = client.putObject(p -> p.bucket(bucket).key(sourceKey).metadata(metadata),
				RequestBody.fromString(content));
		var sourceVid = putResponse.versionId();

		var copyResponse = client.copyObject(c -> c
				.sourceBucket(bucket)
				.sourceKey(sourceKey)
				.sourceVersionId(sourceVid)
				.destinationBucket(bucket)
				.destinationKey(sourceKey2));
		var targetVid = copyResponse.versionId();

		// Backend 클라이언트로 복사
		backendCopyObject(backendClient, bucket, sourceKey2, bucket, targetKey, targetVid, targetVid);

		// 일반 클라이언트로 타겟 오브젝트 확인
		var getResponse = client.getObject(g -> g.bucket(bucket).key(targetKey));
		var body = getBody(getResponse);
		assertEquals(content, body);
		assertEquals(targetVid, getResponse.response().versionId());
		assertEquals(metadata, getResponse.response().metadata());
	}

	/**
	 * CopyObject 메타데이터가 Replace되었을 경우 복제되는지 확인
	 */
	public void testCopyObjectMetadataReplaceReplication() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucket = createBucket(client);
		var sourceKey = "sourceKey";
		var sourceKey2 = "sourceKey2";
		var targetKey = "targetKey";
		var content = "test content";
		var metadata = new HashMap<String, String>();
		metadata.put("testKey", "testValue");
		var metadata2 = new HashMap<String, String>();
		metadata2.put("testKey2", "testValue2");

		// 버킷에 버저닝 활성화
		checkConfigureVersioningRetry(bucket, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 소스 오브젝트 업로드 및 복사
		var putResponse = client.putObject(p -> p.bucket(bucket).key(sourceKey).metadata(metadata),
				RequestBody.fromString(content));
		var sourceVid = putResponse.versionId();

		var copyResponse = client.copyObject(c -> c
				.sourceBucket(bucket)
				.sourceKey(sourceKey)
				.sourceVersionId(sourceVid)
				.destinationBucket(bucket)
				.destinationKey(sourceKey2)
				.metadata(metadata2)
				.metadataDirective(MetadataDirective.REPLACE));
		var targetVid = copyResponse.versionId();

		// Backend 클라이언트로 복사
		backendCopyObject(backendClient, bucket, sourceKey2, bucket, targetKey, targetVid, targetVid);

		// 일반 클라이언트로 타겟 오브젝트 확인
		var getResponse = client.getObject(g -> g.bucket(bucket).key(targetKey));
		var body = getBody(getResponse);
		assertEquals(content, body);
		assertEquals(targetVid, getResponse.response().versionId());
		assertEquals(metadata2, getResponse.response().metadata());
	}

	/**
	 * MultipartUpload 복제가 정상 동작하는지 확인
	 */
	public void testMultipartUploadReplication() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var sourceKey = "testMultipartUploadReplicationSource";
		var targetKey = "testMultipartUploadReplicationTarget";

		var size = 10 * MainData.MB;

		// 버저닝 활성화
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 멀티파트 업로드
		var uploadData = setupMultipartUpload(client, bucketName, sourceKey, size, DEFAULT_PART_SIZE);
		var completeResponse = client
				.completeMultipartUpload(c -> c.bucket(bucketName).key(sourceKey).uploadId(uploadData.uploadId)
						.multipartUpload(p -> p.parts(uploadData.parts)));
		var versionId = completeResponse.versionId();

		// Backend 클라이언트로 멀티파트 업로드
		backendMultipartUpload(backendClient, bucketName, sourceKey, bucketName, targetKey, versionId);

		// 일반 클라이언트로 타겟 오브젝트 확인
		var getResponse = client.headObject(g -> g.bucket(bucketName).key(targetKey).versionId(versionId));
		assertEquals(size, getResponse.contentLength());
		assertEquals(versionId, getResponse.versionId());

		checkContentUsingRange(bucketName, targetKey, versionId, uploadData.getBody(), MainData.MB);
	}

	/**
	 * MultipartUpload 태그가 복제되는지 확인
	 */
	public void testMultipartUploadWithTaggingReplication() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var sourceKey = "testMultipartUploadTaggingReplicationSource";
		var targetKey = "testMultipartUploadTaggingReplicationTarget";

		var size = 10 * MainData.MB;
		var tagging = Tagging.builder().tagSet(Tag.builder().key("testKey").value("testValue").build()).build();

		// 버저닝 활성화
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 멀티파트 업로드 (태그 포함)
		var initUploadData = new MultipartUploadV2Data();
		var createResponse = client.createMultipartUpload(c -> c
				.bucket(bucketName)
				.key(sourceKey)
				.tagging(tagging));
		initUploadData.uploadId = createResponse.uploadId();
		var uploadData = multipartUpload(client, bucketName, sourceKey, size, DEFAULT_PART_SIZE, initUploadData);

		var completeResponse = client
				.completeMultipartUpload(c -> c.bucket(bucketName).key(sourceKey).uploadId(uploadData.uploadId)
						.multipartUpload(p -> p.parts(uploadData.parts)));
		var versionId = completeResponse.versionId();

		// 일반 클라이언트로 태그 확인
		var tagResponse = client.getObjectTagging(g -> g.bucket(bucketName).key(sourceKey));
		tagCompare(tagging.tagSet(), tagResponse.tagSet());

		// Backend 클라이언트로 멀티파트 업로드
		backendMultipartUpload(backendClient, bucketName, sourceKey, bucketName, targetKey, versionId);

		// 일반 클라이언트로 타겟 오브젝트 확인
		var getResponse = client.headObject(g -> g.bucket(bucketName).key(targetKey).versionId(versionId));
		assertEquals(size, getResponse.contentLength());
		assertEquals(versionId, getResponse.versionId());

		// 태그 확인
		tagResponse = client.getObjectTagging(g -> g.bucket(bucketName).key(targetKey));
		tagCompare(tagging.tagSet(), tagResponse.tagSet());

		checkContentUsingRange(bucketName, targetKey, versionId, uploadData.getBody(), MainData.MB);
	}

	/**
	 * MultipartUpload 헤더와 메타데이터가 복제되는지 확인
	 */
	public void testMultipartUploadWithMetadataReplication() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var sourceKey = "testMultipartUploadMetadataReplicationSource";
		var targetKey = "testMultipartUploadMetadataReplicationTarget";

		var size = 10 * MainData.MB;
		var metadata = new HashMap<String, String>();
		metadata.put("testKey", "testValue");

		// 버저닝 활성화
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 멀티파트 업로드 (메타데이터 포함)
		var uploadData = setupMultipartUpload(client, bucketName, sourceKey, size, DEFAULT_PART_SIZE, metadata);
		var completeResponse = client
				.completeMultipartUpload(c -> c.bucket(bucketName).key(sourceKey).uploadId(uploadData.uploadId)
						.multipartUpload(p -> p.parts(uploadData.parts)));
		var versionId = completeResponse.versionId();

		// 일반 클라이언트로 메타데이터 확인
		var metadataResponse = client.headObject(g -> g.bucket(bucketName).key(sourceKey));
		assertEquals(metadata, metadataResponse.metadata());

		// Backend 클라이언트로 멀티파트 업로드
		backendMultipartUpload(backendClient, bucketName, sourceKey, bucketName, targetKey, versionId);

		// 일반 클라이언트로 타겟 오브젝트 확인
		var getResponse = client.headObject(g -> g.bucket(bucketName).key(targetKey).versionId(versionId));
		assertEquals(size, getResponse.contentLength());
		assertEquals(versionId, getResponse.versionId());
		assertEquals(metadata, getResponse.metadata());

		checkContentUsingRange(bucketName, targetKey, versionId, uploadData.getBody(), MainData.MB);
	}

	/**
	 * PutObjectAcl 복제가 정상 동작하는지 확인
	 */
	public void testPutObjectAclReplication() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucketCannedAcl(client);
		var sourceKey = "testPutObjectAclReplicationSource";
		var targetKey = "testPutObjectAclReplicationTarget";
		var content = "test content";

		// 버저닝 활성화
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 업로드
		var putResponse = client.putObject(p -> p.bucket(bucketName).key(sourceKey), RequestBody.fromString(content));
		var versionId = putResponse.versionId();

		// 일반 클라이언트로 ACL 변경
		client.putObjectAcl(p -> p.bucket(bucketName).key(sourceKey).acl(ObjectCannedACL.PUBLIC_READ));

		// Backend 클라이언트로 복사
		backendPutObject(backendClient, bucketName, sourceKey, bucketName, targetKey, versionId);

		// Backend 클라이언트로 ACL 설정
		backendPutObjectAcl(backendClient, bucketName, sourceKey, bucketName, targetKey, versionId);

		// 일반 클라이언트로 복제 확인
		var getResponse = client.getObject(g -> g.bucket(bucketName).key(targetKey));
		var body = getBody(getResponse);
		assertEquals(content, body);
		assertEquals(versionId, getResponse.response().versionId());

		// ACL 확인
		var aclResponse = client.getObjectAcl(g -> g.bucket(bucketName).key(targetKey));
		assertEquals(2, aclResponse.grants().size());
		checkAcl(createPublicAcl(Permission.READ), aclResponse);
	}

	/**
	 * putObjectTagging 복제가 정상 동작하는지 확인
	 */
	public void testPutObjectTaggingReplication() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var sourceKey = "testPutObjectTaggingReplicationSource";
		var targetKey = "testPutObjectTaggingReplicationTarget";
		var content = "test content";
		var tagging = Tagging.builder().tagSet(Tag.builder().key("testKey").value("testValue").build()).build();

		// 버저닝 활성화
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 업로드
		var putResponse = client.putObject(p -> p.bucket(bucketName).key(sourceKey).tagging(tagging),
				RequestBody.fromString(content));
		var versionId = putResponse.versionId();

		// Backend 클라이언트로 복사
		backendPutObject(backendClient, bucketName, sourceKey, bucketName, targetKey,
				versionId);

		// Backend 클라이언트로 태그 설정
		backendPutObjectTagging(backendClient, bucketName, sourceKey, bucketName,
				targetKey, versionId);

		// 일반 클라이언트로 복제 확인
		var getResponse = client.getObject(g -> g.bucket(bucketName).key(targetKey));
		var body = getBody(getResponse);
		assertEquals(content, body);
		assertEquals(versionId, getResponse.response().versionId());

		// 태그 확인
		var tagResponse = client.getObjectTagging(g -> g.bucket(bucketName).key(targetKey));
		tagCompare(tagging.tagSet(), tagResponse.tagSet());
	}

	/**
	 * deleteObject 복제가 정상 동작하는지 확인
	 */
	public void testDeleteObjectReplication() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var sourceKey = "testDeleteObjectReplicationSource";
		var targetKey = "testDeleteObjectReplicationTarget";
		var content = "test content";

		// 버저닝 활성화
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 업로드
		var putResponse = client.putObject(p -> p.bucket(bucketName).key(sourceKey), RequestBody.fromString(content));
		var versionId = putResponse.versionId();

		// Backend 클라이언트로 복사
		backendPutObject(backendClient, bucketName, sourceKey, bucketName, targetKey, versionId);

		// 일반 클라이언트로 삭제
		var deleteResponse = client.deleteObject(d -> d.bucket(bucketName).key(sourceKey));
		var markerVersionId = deleteResponse.versionId();

		// Backend 클라이언트로 삭제
		backendDeleteObject(backendClient, bucketName, targetKey, markerVersionId);

		// 일반 클라이언트로 DeleteMarker 확인
		var listResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		assertEquals(2, listResponse.deleteMarkers().size());

		var sourceMarker = listResponse.deleteMarkers().get(0);
		assertEquals(markerVersionId, sourceMarker.versionId());

		var targetMarker = listResponse.deleteMarkers().get(1);
		assertEquals(markerVersionId, targetMarker.versionId());
	}

	/**
	 * deleteObjectTagging 복제가 정상 동작하는지 확인
	 */
	public void testDeleteObjectTaggingReplication() {
		var client = getClient();
		var backendClient = getBackendClient();
		var bucketName = createBucket(client);
		var sourceKey = "testDeleteObjectTaggingReplicationSource";
		var targetKey = "testDeleteObjectTaggingReplicationTarget";
		var content = "test content";
		var tagging = Tagging.builder().tagSet(Tag.builder().key("testKey").value("testValue").build()).build();

		// 버저닝 활성화
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);

		// 일반 클라이언트로 업로드
		var putResponse = client.putObject(p -> p.bucket(bucketName).key(sourceKey), RequestBody.fromString(content));
		var versionId = putResponse.versionId();

		// Backend 클라이언트로 복사
		backendPutObject(backendClient, bucketName, sourceKey, bucketName, targetKey, versionId);

		// 일반 클라이언트로 태그 설정
		client.putObjectTagging(p -> p.bucket(bucketName).key(sourceKey).tagging(tagging));

		// Backend 클라이언트로 태그 복사
		backendPutObjectTagging(backendClient, bucketName, sourceKey, bucketName, targetKey, versionId);

		// 일반 클라이언트로 태그 확인
		var tagResponse = client.getObjectTagging(g -> g.bucket(bucketName).key(targetKey));
		tagCompare(tagging.tagSet(), tagResponse.tagSet());

		var tagResponse2 = client.getObjectTagging(g -> g.bucket(bucketName).key(sourceKey));
		tagCompare(tagging.tagSet(), tagResponse2.tagSet());

		// 일반 클라이언트로 태그 삭제
		client.deleteObjectTagging(d -> d.bucket(bucketName).key(targetKey));

		// Backend 클라이언트로 태그 삭제
		backendDeleteObjectTagging(backendClient, bucketName, targetKey, versionId);

		// 일반 클라이언트로 태그 확인
		var tagResponse3 = client.getObjectTagging(g -> g.bucket(bucketName).key(sourceKey));
		tagCompare(tagging.tagSet(), tagResponse3.tagSet());

		var tagResponse4 = client.getObjectTagging(g -> g.bucket(bucketName).key(targetKey));
		assertEquals(0, tagResponse4.tagSet().size());
	}
}
