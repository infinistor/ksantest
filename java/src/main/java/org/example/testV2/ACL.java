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
		System.out.println("ACL V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("ACL V2 End");
	}

	@Test
	@Tag("Get")
	public void testObjectRawGet() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PUBLIC_READ, ObjectCannedACL.PUBLIC_READ, key);

		var unauthenticatedClient = getPublicClient();
		unauthenticatedClient.getObject(g -> g.bucket(bucketName).key(key));
	}

	@Test
	@Tag("Get")
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
	public void testObjectRawGetBucketAcl() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PRIVATE, ObjectCannedACL.PUBLIC_READ, key);

		var unauthenticatedClient = getPublicClient();
		unauthenticatedClient.getObject(g -> g.bucket(bucketName).key(key));
	}

	@Test
	@Tag("Get")
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
	public void testObjectRawAuthenticated() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PUBLIC_READ, ObjectCannedACL.PUBLIC_READ,
				key);

		var client = getClient();
		client.getObject(g -> g.bucket(bucketName).key(key));
	}

	@Test
	@Tag("Header")
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
	public void testObjectRawAuthenticatedBucketAcl() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PRIVATE, ObjectCannedACL.PUBLIC_READ, key);

		var client = getAltClient();
		client.getObject(g -> g.bucket(bucketName).key(key));
	}

	@Test
	@Tag("Get")
	public void testObjectRawAuthenticatedObjectAcl() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PUBLIC_READ, ObjectCannedACL.AUTHENTICATED_READ, key);

		var client = getAltClient();
		client.getObject(g -> g.bucket(bucketName).key(key));
	}

	@Test
	@Tag("Get")
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
	public void testObjectRawGetXAmzExpiresOutRangeZero() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PUBLIC_READ, ObjectCannedACL.PUBLIC_READ, key);
		var presigner = getS3Presigner();

		var presignedGetObjectRequest = presigner
				.presignGetObject(z -> z
						.signatureDuration(Duration.ofSeconds(1))
						.getObjectRequest(g -> g.bucket(bucketName).key(key)));

		delay(1000);

		var response = getObject(presignedGetObjectRequest.url());
		assertEquals(403, response.getStatusLine().getStatusCode());
	}

	@Test
	@Tag("Post")
	public void testObjectRawGetXAmzExpiresOutPositiveRange() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(BucketCannedACL.PUBLIC_READ, ObjectCannedACL.PUBLIC_READ, key);
		var presigner = getS3Presigner();

		var presignedGetObjectRequest = presigner
				.presignGetObject(z -> z
						.signatureDuration(Duration.ofSeconds(1))
						.getObjectRequest(g -> g.bucket(bucketName).key(key)));

		delay(1000);
		var response = getObject(presignedGetObjectRequest.url());
		assertEquals(403, response.getStatusLine().getStatusCode());
	}

	@Test
	@Tag("Update")
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
	public void testObjectPutAuthenticated() {
		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(p -> p.bucket(bucketName).key("foo"),
				RequestBody.fromString("foo"));
	}

	@Test
	@Tag("Default")
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
