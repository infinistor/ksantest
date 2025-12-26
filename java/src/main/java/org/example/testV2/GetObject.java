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

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;

public class GetObject extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("GetObject V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("GetObject V2 End");
	}

	@Test
	@Tag("ERROR")
	public void testObjectReadNotExist() {
		var client = getClient();
		var bucketName = createBucket(client);

		var e = assertThrows(AwsServiceException.class,
				() -> client.getObject(g -> g.bucket(bucketName).key("foo")));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("IfMatch")
	public void testGetObjectIfMatchGood() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";

		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));
		var eTag = putResponse.eTag();

		var getResponse = client.getObject(g -> g.bucket(bucketName).key(key).ifMatch(eTag));
		var body = getBody(getResponse);
		assertEquals("bar", body);
	}

	@Test
	@Tag("IfMatch")
	public void testGetObjectIfMatchFailed() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));
		var e = assertThrows(AwsServiceException.class, () -> client
				.getObject(g -> g.bucket(bucketName).key(key).ifMatch("ABCDEFGHIJKLMNOPQRSTUVWXYZ")));

		assertEquals(HttpStatus.SC_PRECONDITION_FAILED, e.statusCode());
		assertEquals(MainData.PRECONDITION_FAILED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("IfNoneMatch")
	public void testGetObjectIfNoneMatchGood() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";

		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));
		var eTag = putResponse.eTag();

		var e = assertThrows(AwsServiceException.class,
				() -> client.getObject(g -> g.bucket(bucketName).key(key).ifNoneMatch(eTag)));

		assertEquals(HttpStatus.SC_NOT_MODIFIED, e.statusCode());
		assertNull(e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("IfNoneMatch")
	public void testGetObjectIfNoneMatchFailed() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));

		var getResponse = client
				.getObject(g -> g.bucket(bucketName).key(key).ifNoneMatch("ABCDEFGHIJKLMNOPQRSTUVWXYZ"));
		var body = getBody(getResponse);
		assertEquals("bar", body);
	}

	@Test
	@Tag("IfModifiedSince")
	public void testGetObjectIfModifiedSinceGood() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));

		var days = Calendar.getInstance();
		days.set(1994, 8, 29, 19, 43, 31);
		var response = client
				.getObject(g -> g.bucket(bucketName).key(key).ifModifiedSince(days.toInstant()));
		var body = getBody(response);
		assertEquals("bar", body);
	}

	@Test
	@Tag("IfModifiedSince")
	public void testGetObjectIfModifiedSinceFailed() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));

		var response = client.getObject(g -> g.bucket(bucketName).key(key));

		var after = response.response().lastModified().plus(1, ChronoUnit.SECONDS);

		delay(1000);

		var e = assertThrows(AwsServiceException.class, () -> client
				.getObject(g -> g.bucket(bucketName).key(key).ifModifiedSince(after)));

		assertEquals(HttpStatus.SC_NOT_MODIFIED, e.statusCode());
		assertNull(e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ifUnmodifiedSince")
	public void testGetObjectIfUnmodifiedSinceGood() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));

		var days = Calendar.getInstance();
		days.set(1994, 8, 29, 19, 43, 31);

		var e = assertThrows(AwsServiceException.class, () -> client
				.getObject(g -> g.bucket(bucketName).key(key).ifUnmodifiedSince(days.toInstant())));

		assertEquals(HttpStatus.SC_PRECONDITION_FAILED, e.statusCode());
		assertEquals(MainData.PRECONDITION_FAILED, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ifUnmodifiedSince")
	public void testGetObjectIfUnmodifiedSinceFailed() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));

		var days = Calendar.getInstance();
		days.set(2100, 8, 29, 19, 43, 31);
		var response = client
				.getObject(g -> g.bucket(bucketName).key(key).ifUnmodifiedSince(days.toInstant()));
		var body = getBody(response);
		assertEquals("bar", body);
	}

	@Test
	@Tag("Range")
	public void testRangedRequestResponseCode() {
		var key = "obj";
		var content = "contentData";

		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));
		var response = client.getObject(g -> g.bucket(bucketName).key(key).range("bytes=4-7"));

		var fetchedContent = getBody(response);
		assertEquals(content.substring(4, 8), fetchedContent);
		assertEquals("bytes 4-7/11", response.response().contentRange());
	}

	@Test
	@Tag("Range")
	public void testRangedBigRequestResponseCode() {
		var key = "obj";
		var content = Utils.randomTextToLong(8 * MainData.MB);

		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));
		var response = client.getObject(g -> g.bucket(bucketName).key(key).range("bytes=3145728-5242880"));

		var fetchedContent = getBody(response);
		assertTrue(content.substring(3145728, 5242881).equals(fetchedContent), MainData.NOT_MATCHED);
		assertEquals("bytes 3145728-5242880/8388608",
				response.response().contentRange());
	}

	@Test
	@Tag("Range")
	public void testRangedRequestSkipLeadingBytesResponseCode() {
		var key = "obj";
		var content = "contentData";

		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));
		var response = client.getObject(g -> g.bucket(bucketName).key(key).range("bytes=4-"));

		var fetchedContent = getBody(response);
		assertEquals(content.substring(4), fetchedContent);
		assertEquals("bytes 4-10/11", response.response().contentRange());
	}

	@Test
	@Tag("Range")
	public void testRangedRequestReturnTrailingBytesResponseCode() {
		var key = "obj";
		var content = "contentData";

		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));
		var response = client.getObject(g -> g.bucket(bucketName).key(key).range("bytes=-7"));

		var fetchedContent = getBody(response);
		assertEquals(content.substring(content.length() - 7), fetchedContent);
		assertEquals("bytes 4-10/11", response.response().contentRange());
	}

	@Test
	@Tag("Range")
	public void testRangedRequestInvalidRange() {
		var key = "obj";
		var content = "contentData";

		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));
		var e = assertThrows(AwsServiceException.class,
				() -> client.getObject(g -> g.bucket(bucketName).key(key).range("bytes=40-50")));
		assertEquals(HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE, e.statusCode());
		assertEquals(MainData.INVALID_RANGE, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Range")
	public void testRangedRequestEmptyObject() {
		var key = "obj";
		var content = "";

		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));
		var e = assertThrows(AwsServiceException.class,
				() -> client.getObject(g -> g.bucket(bucketName).key(key).range("bytes=40-50")));
		assertEquals(HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE, e.statusCode());
		assertEquals(MainData.INVALID_RANGE, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Get")
	public void testGetObjectMany() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";
		var data = Utils.randomTextToLong(15 * MainData.MB);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(data));
		checkContent(bucketName, key, data, 50);
	}

	@Test
	@Tag("Get")
	public void testRangeObjectMany() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";
		var fileSize = 1024 * 1024 * 15;
		var data = Utils.randomTextToLong(fileSize);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(data));
		checkContentUsingRandomRange(bucketName, key, data, 50);
	}

	@Test
	@Tag("Header")
	public void testObjectResponseHeaders() {
		var key = "testObjectResponseHeaders";
		var client = getClient();
		var bucketName = createObjects(client, key);

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

	// 멀티파트로 업로드 된 오브젝트를 다운로드 할때 파트 번호를 지정하여 다운로드 가능한지 확인
	@Test
	@Tag("Range")
	public void testMultipartObjectRange() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testMultipartObjectRange";

		var multipartUploadData = multipartUpload(client, bucketName, key, 5 * MainData.MB, 5 * MainData.MB);

		var response = client.getObject(g -> g.bucket(bucketName).key(key).partNumber(1));
		var body = getBody(response);
		assertEquals(multipartUploadData.getBody().substring(0, 5 * MainData.MB), body);
	}

	@Test
	@Tag("Get")
	public void testGetObjectIgnore() {
		var key = "testObjectIgnore";
		var client = getClient();
		var bucketName = createObjects(client, key);

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals(key.length(), response.response().contentLength());
	}

	@Test
	@Tag("ERROR")
	public void testGetObjectAfterDelete() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testGetObjectAfterDelete";
		var body = "testContent";

		// 오브젝트 업로드
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(body));

		// GetObject로 업로드 확인
		var getResponse = client.getObject(g -> g.bucket(bucketName).key(key));
		var fetchedBody = getBody(getResponse);
		assertEquals(body, fetchedBody);
		assertEquals(body.length(), getResponse.response().contentLength());

		// 오브젝트 삭제
		client.deleteObject(d -> d.bucket(bucketName).key(key));

		// 삭제 후 GetObject 실패 확인
		var e = assertThrows(AwsServiceException.class,
				() -> client.getObject(g -> g.bucket(bucketName).key(key)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("ERROR")
	public void testGetObjectAfterDeleteVersioning() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testGetObjectAfterDeleteVersioning";
		var body = "testContent";

		// 버저닝 활성화
		client.putBucketVersioning(p -> p.bucket(bucketName)
				.versioningConfiguration(
						v -> v.status(BucketVersioningStatus.ENABLED)));

		// 오브젝트 업로드
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(body));

		// GetObject로 업로드 확인
		var getResponse = client.getObject(g -> g.bucket(bucketName).key(key));
		var fetchedBody = getBody(getResponse);
		assertEquals(body, fetchedBody);
		assertEquals(body.length(), getResponse.response().contentLength());

		// 오브젝트 삭제 (DeleteMarker 생성)
		client.deleteObject(d -> d.bucket(bucketName).key(key));

		// 삭제 후 GetObject 실패 확인
		var e = assertThrows(AwsServiceException.class,
				() -> client.getObject(g -> g.bucket(bucketName).key(key)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Versioning")
	public void testGetObjectDeleteMarker() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testGetObjectDeleteMarker";
		var body = "testContent";

		// 버저닝 활성화
		client.putBucketVersioning(p -> p.bucket(bucketName)
				.versioningConfiguration(
						v -> v.status(BucketVersioningStatus.ENABLED)));

		// 오브젝트 업로드
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(body));

		// GetObject로 업로드 확인
		var getResponse = client.getObject(g -> g.bucket(bucketName).key(key));
		var fetchedBody = getBody(getResponse);
		assertEquals(body, fetchedBody);

		// 오브젝트 삭제 (DeleteMarker 생성)
		client.deleteObject(d -> d.bucket(bucketName).key(key));

		// ListObjectVersions로 버전 목록 조회
		var listResponse = client.listObjectVersions(l -> l.bucket(bucketName));
		var versions = listResponse.versions();
		var deleteMarkers = listResponse.deleteMarkers();

		// DeleteMarker 1개, 실제 오브젝트 1개가 있어야 함
		assertEquals(1, deleteMarkers.size());
		assertEquals(1, versions.size());

		// DeleteMarker로 GetObject 시도 - 실패
		var deleteMarker = deleteMarkers.get(0);
		var e = assertThrows(AwsServiceException.class,
				() -> client.getObject(g -> g.bucket(bucketName).key(key).versionId(deleteMarker.versionId())));
		assertEquals(HttpStatus.SC_METHOD_NOT_ALLOWED, e.statusCode());

		// 실제 오브젝트 버전으로 GetObject - 성공
		var version = versions.get(0);
		var response = client.getObject(g -> g.bucket(bucketName).key(key).versionId(version.versionId()));
		var content = getBody(response);
		assertEquals(body, content);
	}
}
