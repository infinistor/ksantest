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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.ExpirationStatus;

public class DeleteBucket extends TestBase {
	@Test
	@Tag("ERROR")
	public void testBucketDeleteNotExist() {
		var bucketName = getNewBucketNameOnly(1);
		var client = getClient();

		var e = assertThrows(AwsServiceException.class, () -> client.deleteBucket(d -> d.bucket(bucketName)));

		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testBucketDeleteNonempty() {
		var client = getClient();
		var bucketName = createObjects(client, 2, "foo");

		var e = assertThrows(AwsServiceException.class, () -> client.deleteBucket(d -> d.bucket(bucketName)));

		assertEquals(HttpStatus.SC_CONFLICT, e.statusCode());
		assertEquals(MainData.BUCKET_NOT_EMPTY, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testBucketCreateDelete() {
		var client = getClient();
		var bucketName = createBucket(client, 3);

		client.deleteBucket(d -> d.bucket(bucketName));

		var e = assertThrows(AwsServiceException.class, () -> client.deleteBucket(d -> d.bucket(bucketName)));

		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
		deleteBucketList(bucketName);
	}

	@Test
	@Tag("Versioning")
	public void testBucketDeleteVersioned() {
		var client = getClient();
		var bucketName = createBucket(client, 4);
		var key = "testBucketDeleteVersioned";
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));
		client.deleteObject(d -> d.bucket(bucketName).key(key));

		// 현재 오브젝트가 없어도 이전 버전과 삭제 마커가 남아 있으면 삭제할 수 없다
		assertEquals(0, client.listObjectsV2(l -> l.bucket(bucketName)).keyCount());
		var e = assertThrows(AwsServiceException.class, () -> client.deleteBucket(d -> d.bucket(bucketName)));
		assertEquals(HttpStatus.SC_CONFLICT, e.statusCode());
		assertEquals(MainData.BUCKET_NOT_EMPTY, e.awsErrorDetails().errorCode());

		var versions = client.listObjectVersions(l -> l.bucket(bucketName));
		for (var version : versions.versions())
			client.deleteObject(d -> d.bucket(bucketName).key(version.key()).versionId(version.versionId()));
		for (var marker : versions.deleteMarkers())
			client.deleteObject(d -> d.bucket(bucketName).key(marker.key()).versionId(marker.versionId()));
		client.deleteBucket(d -> d.bucket(bucketName));
		deleteBucketList(bucketName);
	}

	@Test
	@Tag("Multipart")
	public void testBucketDeleteWithMultipartUpload() {
		var client = getClient();
		var bucketName = createBucket(client, 5);
		var key = "testBucketDeleteWithMultipartUpload";
		var upload = client.createMultipartUpload(c -> c.bucket(bucketName).key(key));
		client.uploadPart(u -> u.bucket(bucketName).key(key).uploadId(upload.uploadId()).partNumber(1),
				RequestBody.fromString("a".repeat(1024)));

		// 완료되지 않은 멀티파트 업로드만 남아 있는 버킷은 삭제할 수 있다
		client.deleteBucket(d -> d.bucket(bucketName));
		var e = assertThrows(AwsServiceException.class, () -> client.listObjectsV2(l -> l.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
		deleteBucketList(bucketName);
	}

	@Test
	@Tag("Permission")
	public void testBucketDeleteAltUser() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 6);

		var e = assertThrows(AwsServiceException.class, () -> altClient.deleteBucket(d -> d.bucket(bucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
		// 버킷은 그대로 남아 있다
		client.headBucket(h -> h.bucket(bucketName));
	}

	@SuppressWarnings("unchecked")
	@Test
	@Tag("Check")
	public void testBucketDeleteWithConfigurations() {
		var client = getClient();
		var bucketName = createBucket(client, 7);
		client.putBucketTagging(
				p -> p.bucket(bucketName).tagging(t -> t.tagSet(s -> s.key("key").value("value"))));
		client.putBucketCors(p -> p.bucket(bucketName)
				.corsConfiguration(c -> c.corsRules(r -> r.allowedMethods("GET").allowedOrigins("*"))));
		client.putBucketLifecycleConfiguration(p -> p.bucket(bucketName)
				.lifecycleConfiguration(l -> l.rules(r -> r.id("rule1").status(ExpirationStatus.ENABLED)
						.filter(f -> f.prefix("")).expiration(x -> x.days(1)))));

		// 버킷 설정이 남아 있어도 비어 있으면 삭제할 수 있다
		client.deleteBucket(d -> d.bucket(bucketName));
		var e = assertThrows(AwsServiceException.class, () -> client.listObjectsV2(l -> l.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
		deleteBucketList(bucketName);
	}

	@SuppressWarnings("unchecked")
	@Test
	@Tag("Check")
	public void testBucketDeleteRecreate() {
		var client = getClient();
		var bucketName = createBucket(client, 8);
		var key = "testBucketDeleteRecreate";
		client.putBucketTagging(
				p -> p.bucket(bucketName).tagging(t -> t.tagSet(s -> s.key("key").value("value"))));
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(key));
		client.deleteObject(d -> d.bucket(bucketName).key(key));
		client.deleteBucket(d -> d.bucket(bucketName));

		// 같은 이름으로 다시 만들면 이전 오브젝트와 설정이 남아 있지 않다
		client.createBucket(c -> c.bucket(bucketName));
		assertEquals(0, client.listObjectsV2(l -> l.bucket(bucketName)).keyCount());
		var e = assertThrows(AwsServiceException.class, () -> client.getBucketTagging(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_TAG_SET, e.awsErrorDetails().errorCode());
	}
}
