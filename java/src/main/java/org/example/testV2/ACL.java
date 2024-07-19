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

import java.time.Duration;
import java.time.Instant;

import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

public class ACL extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("ACL Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("ACL End");
	}

	@Test
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 오브젝트에 접근 가능한지
	// 확인
	public void testObjectRawGet() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PUBLIC_READ, ObjectCannedACL.PUBLIC_READ, key);

		var unauthenticatedClient = getPublicClient();
		unauthenticatedClient.getObject(g -> g.bucket(bucketName).key(key));
	}

	@Test
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 삭제된 버킷의 삭제된
	// 오브젝트에 접근할때 에러 확인
	public void testObjectRawGetBucketGone() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PUBLIC_READ, ObjectCannedACL.PUBLIC_READ, key);
		var client = getClient();

		client.deleteObject(d -> d.bucket(bucketName).key(key));
		client.deleteBucket(d -> d.bucket(bucketName));

		var unauthenticatedClient = getPublicClient();
		var e = assertThrows(AwsServiceException.class,
				() -> unauthenticatedClient.getObject(g -> g.bucket(bucketName).key(key)));

		assertEquals(404, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
		deleteBucketList(bucketName);
	}

	@Test
	@Tag("Delete")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 삭제된 버킷의 삭제된
	// 오브젝트를 삭제할때 에러 확인
	public void testObjectDeleteKeyBucketGone() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PUBLIC_READ, ObjectCannedACL.PUBLIC_READ,
				key);
		var client = getClient();

		client.deleteObject(d -> d.bucket(bucketName).key(key));
		client.deleteBucket(d -> d.bucket(bucketName));

		var unauthenticatedClient = getPublicClient();
		var e = assertThrows(AwsServiceException.class, () -> unauthenticatedClient
				.deleteObject(d -> d.bucket(bucketName).key(key)));

		assertEquals(404, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
		deleteBucketList(bucketName);
	}

	@Test
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 삭제된 오브젝트에 접근할때
	// 에러 확인
	public void testObjectRawGetObjectGone() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PUBLIC_READ, ObjectCannedACL.PUBLIC_READ,
				key);
		var client = getClient();

		client.deleteObject(d -> d.bucket(bucketName).key(key));

		var unauthenticatedClient = getPublicClient();
		var e = assertThrows(AwsServiceException.class,
				() -> unauthenticatedClient.getObject(g -> g.bucket(bucketName).key(key)));

		assertEquals(404, e.statusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Get")
	// [Bucket_ACL = private, Object_ACL = public-read] 권한없는 사용자가 개인버킷의 공용 오브젝트에 접근
	// 가능한지 확인
	public void testObjectRawGetBucketAcl() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PRIVATE, ObjectCannedACL.PUBLIC_READ, key);

		var unauthenticatedClient = getPublicClient();
		unauthenticatedClient.getObject(g -> g.bucket(bucketName).key(key));
	}

	@Test
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = private] 권한없는 사용자가 공용버킷의 개인 오브젝트에
	// 접근할때 에러확인
	public void testObjectRawGetObjectAcl() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PUBLIC_READ, ObjectCannedACL.PRIVATE, key);

		var unauthenticatedClient = getPublicClient();
		var e = assertThrows(AwsServiceException.class,
				() -> unauthenticatedClient.getObject(g -> g.bucket(bucketName).key(key)));

		assertEquals(403, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 로그인한 사용자가 공용 버킷의 공용
	// 오브젝트에 접근 가능한지 확인
	public void testObjectRawAuthenticated() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PUBLIC_READ, ObjectCannedACL.PUBLIC_READ,
				key);

		var client = getClient();
		client.getObject(g -> g.bucket(bucketName).key(key));
	}

	@Test
	@Tag("Header")
	// [Bucket_ACL = private, Object_ACL = private] 로그인한 사용자가 GetObject의 반환헤더값을 설정하고
	// 개인 오브젝트를 가져올때 반환헤더값이 적용되었는지 확인
	public void testObjectRawResponseHeaders() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PRIVATE, ObjectCannedACL.PRIVATE, key);
		var client = getClient();

		var response = client.getObject(
				g -> g
						.bucket(bucketName)
						.key(key)
						.responseCacheControl("no-cache")
						.responseContentDisposition("bla")
						.responseContentEncoding("aaa")
						.responseContentLanguage("esperanto")
						.responseContentType("foo/bar")
						.responseExpires(Instant.now()));

		assertEquals("no-cache", response.response().cacheControl());
		assertEquals("bla", response.response().contentDisposition());
		assertEquals("aaa", response.response().contentEncoding());
		assertEquals("esperanto", response.response().contentLanguage());
		assertEquals("foo/bar", response.response().contentType());
	}

	@Test
	@Tag("Get")
	// [Bucket_ACL = private, Object_ACL = public-read] 로그인한 사용자가 개인버킷의 공용 오브젝트에 접근
	// 가능한지 확인
	public void testObjectRawAuthenticatedBucketAcl() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PRIVATE, ObjectCannedACL.PUBLIC_READ, key);

		var client = getAltClient();
		client.getObject(g -> g.bucket(bucketName).key(key));
	}

	@Test
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = private] 로그인한 사용자가 공용버킷의 개인 오브젝트에 접근
	// 가능한지 확인
	public void testObjectRawAuthenticatedObjectAcl() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PUBLIC_READ, ObjectCannedACL.AUTHENTICATED_READ, key);

		var client = getAltClient();
		client.getObject(g -> g.bucket(bucketName).key(key));
	}

	@Test
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 로그인한 사용자가 삭제된 버킷의 삭제된
	// 오브젝트에 접근할때 에러 확인
	public void testObjectRawAuthenticatedBucketGone() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PUBLIC_READ, ObjectCannedACL.PUBLIC_READ,
				key);
		var client = getClient();

		client.deleteObject(d -> d.bucket(bucketName).key(key));
		client.deleteBucket(d -> d.bucket(bucketName));

		var e = assertThrows(AwsServiceException.class,
				() -> client.getObject(g -> g.bucket(bucketName).key(key)));

		assertEquals(404, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
		deleteBucketList(bucketName);
	}

	@Test
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 로그인한 사용자가 삭제된 오브젝트에 접근할때
	// 에러 확인
	public void testObjectRawAuthenticatedObjectGone() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PUBLIC_READ, ObjectCannedACL.PUBLIC_READ,
				key);
		var client = getClient();

		client.deleteObject(d -> d.bucket(bucketName).key(key));

		var e = assertThrows(AwsServiceException.class,
				() -> client.getObject(g -> g.bucket(bucketName).key(key)));

		assertEquals(404, e.statusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Post")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 로그인이 만료되지 않은 사용자가 공용 버킷의
	// 공용 오브젝트에 URL 형식으로 접근 가능한지 확인
	public void testObjectRawGetXAmzExpiresNotExpired() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PUBLIC_READ, ObjectCannedACL.PUBLIC_READ, key);
		var presigner = getS3Presigner();

		var presignedGetObjectRequest = presigner
				.presignGetObject(z -> z
						.signatureDuration(Duration.ofMinutes(10))
						.getObjectRequest(g -> g.bucket(bucketName).key(key)));

		var address = presignedGetObjectRequest.url();
		var response = getObject(address);

		assertEquals(200, response.getStatusLine().getStatusCode());
	}

	@Test
	@Tag("Post")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 로그인이 만료된 사용자가 공용 버킷의 공용
	// 오브젝트에 URL 형식으로 접근 실패 확인
	public void testObjectRawGetXAmzExpiresOutRangeZero() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PUBLIC_READ, ObjectCannedACL.PUBLIC_READ, key);
		var presigner = getS3Presigner();

		var presignedGetObjectRequest = presigner
				.presignGetObject(z -> z
						.signatureDuration(Duration.ofSeconds(1))
						.getObjectRequest(g -> g.bucket(bucketName).key(key)));

		// Wait for the presigned URL to expire
		delay(1000);

		var response = getObject(presignedGetObjectRequest.url());
		assertEquals(403, response.getStatusLine().getStatusCode());
	}

	@Test
	@Tag("Post")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 로그인 유효주기가 만료된 사용자가 공용
	// 버킷의 공용 오브젝트에 URL 형식으로 접근 실패 확인
	public void testObjectRawGetXAmzExpiresOutPositiveRange() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PUBLIC_READ, ObjectCannedACL.PUBLIC_READ, key);
		var presigner = getS3Presigner();

		var presignedGetObjectRequest = presigner
				.presignGetObject(z -> z
						.signatureDuration(Duration.ofSeconds(1))
						.getObjectRequest(g -> g.bucket(bucketName).key(key)));

		// Wait for the presigned URL to expire
		delay(1000);
		var response = getObject(presignedGetObjectRequest.url());
		assertEquals(403, response.getStatusLine().getStatusCode());
	}

	@Test
	@Tag("Update")
	// [Bucket_ACL = Default, Object_ACL = Default] 로그인한 사용자가 버켓을 만들고 업로드한 오브젝트를
	// 권한없는 사용자가 업데이트하려고 할때 실패 확인
	public void testObjectAnonPut() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.empty());

		var unauthenticatedClient = getPublicClient();

		var e = assertThrows(AwsServiceException.class,
				() -> unauthenticatedClient.putObject(p -> p.bucket(bucketName).key(key),
						RequestBody.fromString("bar")));
		assertEquals(403, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Update")
	// [Bucket_ACL = public-read-write] 로그인한 사용자가 공용버켓(w/r)을 만들고 업로드한 오브젝트를 권한없는
	// 사용자가 업데이트했을때 올바르게 적용 되는지 확인
	public void testObjectAnonPutWriteAccess() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.empty());

		var unauthenticatedClient = getPublicClient();

		var e = assertThrows(AwsServiceException.class,
				() -> unauthenticatedClient.putObject(p -> p.bucket(bucketName).key(key),
						RequestBody.fromString("bar")));
		assertEquals(403, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Default")
	// [Bucket_ACL = Default, Object_ACL = Default] 로그인한 사용자가 버켓을 만들고 업로드
	public void testObjectPutAuthenticated() {
		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(p -> p.bucket(bucketName).key("foo"),
				RequestBody.fromString("foo"));
	}

	@Test
	@Tag("Default")
	// [Bucket_ACL = Default, Object_ACL = Default] Post방식으로 만료된 로그인 정보를 설정하여 오브젝트
	// 업데이트 실패 확인
	public void testObjectRawPutAuthenticatedExpired() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.empty());

		var presigner = getS3Presigner();

		var presignedPutObjectRequest = presigner
				.presignPutObject(z -> z
						.signatureDuration(Duration.ofSeconds(1))
						.putObjectRequest(p -> p.bucket(bucketName).key(key)));

		delay(1000);
		var response = putObject(presignedPutObjectRequest.url(), null);
		assertEquals(403, response.getStatusLine().getStatusCode());
	}

	@Test
	@Tag("Get")
	// [Bucket_ACL = private, Object_ACL = public-read] 모든 사용자가 개인버킷의 공용 오브젝트에 접근
	// 가능한지 확인
	public void testAclPrivateBucketPublicReadObject() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PRIVATE, ObjectCannedACL.PUBLIC_READ, key);

		var client = getClient();
		aclTest(bucketName, key, client, true);

		var altClient = getAltClient();
		aclTest(bucketName, key, altClient, true);

		var unauthenticatedClient = getPublicClient();
		aclTest(bucketName, key, unauthenticatedClient, true);
	}
}
