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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.BucketCrossOriginConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketTaggingConfiguration;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CORSRule.AllowedMethods;
import com.amazonaws.services.s3.model.CORSRule;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.TagSet;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecyclePrefixPredicate;

public class DeleteBucket extends TestBase {
	@Test
	@Tag("ERROR")
	public void testBucketDeleteNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(1);

		var e = assertThrows(AmazonServiceException.class, () -> client.deleteBucket(bucketName));

		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testBucketDeleteNonempty() {
		var client = getClient();
		var bucketName = createObjects(client, 2, "foo");

		var e = assertThrows(AmazonServiceException.class, () -> client.deleteBucket(bucketName));

		assertEquals(HttpStatus.SC_CONFLICT, e.getStatusCode());
		assertEquals(MainData.BUCKET_NOT_EMPTY, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testBucketCreateDelete() {
		var client = getClient();
		var bucketName = createBucket(client, 3);

		client.deleteBucket(bucketName);

		var e = assertThrows(AmazonServiceException.class, () -> client.deleteBucket(bucketName));

		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());
		deleteBucketList(bucketName);
	}

	@Test
	@Tag("Versioning")
	public void testBucketDeleteVersioned() {
		var client = getClient();
		var bucketName = createBucket(client, 4);
		var key = "testBucketDeleteVersioned";
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		client.putObject(bucketName, key, key);
		client.deleteObject(bucketName, key);

		// 현재 오브젝트가 없어도 이전 버전과 삭제 마커가 남아 있으면 삭제할 수 없다
		assertEquals(0, client.listObjectsV2(bucketName).getKeyCount());
		var e = assertThrows(AmazonServiceException.class, () -> client.deleteBucket(bucketName));
		assertEquals(HttpStatus.SC_CONFLICT, e.getStatusCode());
		assertEquals(MainData.BUCKET_NOT_EMPTY, e.getErrorCode());

		// SDK v1 은 버전과 삭제 마커를 한 목록(VersionSummaries)으로 돌려준다
		var versions = client.listVersions(bucketName, null);
		for (var version : versions.getVersionSummaries())
			client.deleteVersion(bucketName, version.getKey(), version.getVersionId());
		client.deleteBucket(bucketName);
		deleteBucketList(bucketName);
	}

	@Test
	@Tag("Multipart")
	public void testBucketDeleteWithMultipartUpload() {
		var client = getClient();
		var bucketName = createBucket(client, 5);
		var key = "testBucketDeleteWithMultipartUpload";
		var upload = client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key));
		client.uploadPart(new UploadPartRequest().withBucketName(bucketName).withKey(key)
				.withUploadId(upload.getUploadId()).withPartNumber(1)
				.withInputStream(createBody("a".repeat(1024))).withPartSize(1024));

		// 완료되지 않은 멀티파트 업로드만 남아 있는 버킷은 삭제할 수 있다
		client.deleteBucket(bucketName);
		var e = assertThrows(AmazonServiceException.class, () -> client.listObjectsV2(bucketName));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());
		deleteBucketList(bucketName);
	}

	@Test
	@Tag("Permission")
	public void testBucketDeleteAltUser() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 6);

		var e = assertThrows(AmazonServiceException.class, () -> altClient.deleteBucket(bucketName));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());
		// 버킷은 그대로 남아 있다
		client.headBucket(new HeadBucketRequest(bucketName));
	}

	@Test
	@Tag("Check")
	public void testBucketDeleteWithConfigurations() {
		var client = getClient();
		var bucketName = createBucket(client, 7);
		client.setBucketTaggingConfiguration(bucketName,
				new BucketTaggingConfiguration().withTagSets(new TagSet(Map.of("key", "value"))));
		client.setBucketCrossOriginConfiguration(bucketName, new BucketCrossOriginConfiguration().withRules(
				new CORSRule().withAllowedMethods(List.of(AllowedMethods.GET)).withAllowedOrigins(List.of("*"))));
		client.setBucketLifecycleConfiguration(bucketName,
				new BucketLifecycleConfiguration().withRules(new BucketLifecycleConfiguration.Rule().withId("rule1")
						.withStatus(BucketLifecycleConfiguration.ENABLED)
						.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate("")))
						.withExpirationInDays(1)));

		// 버킷 설정이 남아 있어도 비어 있으면 삭제할 수 있다
		client.deleteBucket(bucketName);
		var e = assertThrows(AmazonServiceException.class, () -> client.listObjectsV2(bucketName));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());
		deleteBucketList(bucketName);
	}

	@Test
	@Tag("Check")
	public void testBucketDeleteRecreate() {
		var client = getClient();
		var bucketName = createBucket(client, 8);
		var key = "testBucketDeleteRecreate";
		client.setBucketTaggingConfiguration(bucketName,
				new BucketTaggingConfiguration().withTagSets(new TagSet(Map.of("key", "value"))));
		client.putObject(bucketName, key, key);
		client.deleteObject(bucketName, key);
		client.deleteBucket(bucketName);

		// 같은 이름으로 다시 만들면 이전 오브젝트와 설정이 남아 있지 않다
		client.createBucket(bucketName);
		assertEquals(0, client.listObjectsV2(bucketName).getKeyCount());
		// SDK v1 은 NoSuchTagSet(404) 응답을 예외로 던지지 않고 null 로 돌려준다
		assertNull(client.getBucketTaggingConfiguration(bucketName));
	}
}
