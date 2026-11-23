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
import org.example.Utility.CheckSum;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import java.util.List;
import software.amazon.awssdk.services.s3.S3Client;

public class GetObject extends TestBase {
	@Test
	@Tag("ERROR")
	public void testObjectReadNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 1);

		var e = assertThrows(AwsServiceException.class,
				() -> client.getObject(g -> g.bucket(bucketName).key("testObjectReadNotExist")));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("IfMatch")
	public void testGetObjectIfMatchGood() {
		var client = getClient();
		var bucketName = createBucket(client, 2);
		var key = "testGetObjectIfMatchGood";

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
		var bucketName = createBucket(client, 3);
		var key = "testGetObjectIfMatchFailed";

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
		var bucketName = createBucket(client, 4);
		var key = "testGetObjectIfNoneMatchGood";

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
		var bucketName = createBucket(client, 5);
		var key = "testGetObjectIfNoneMatchFailed";

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
		var bucketName = createBucket(client, 6);
		var key = "testGetObjectIfModifiedSinceGood";

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
		var bucketName = createBucket(client, 7);
		var key = "testGetObjectIfModifiedSinceFailed";

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
		var bucketName = createBucket(client, 8);
		var key = "testGetObjectIfUnmodifiedSinceGood";

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
		var bucketName = createBucket(client, 9);
		var key = "testGetObjectIfUnmodifiedSinceFailed";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));

		var days = Calendar.getInstance();
		days.set(2100, 8, 29, 19, 43, 31);
		var response = client
				.getObject(g -> g.bucket(bucketName).key(key).ifUnmodifiedSince(days.toInstant()));
		var body = getBody(response);
		assertEquals("bar", body);
	}

	@Test
	@Tag("IfMatch")
	// If-Match(일치)와 If-Unmodified-Since(불일치)를 함께 사용할 경우 ETag 조건이 우선되어 성공하는지 확인
	public void testGetObjectIfMatchWithIfUnmodifiedSince() {
		var client = getClient();
		var bucketName = createBucket(client, 10);
		var key = "testGetObjectIfMatchWithIfUnmodifiedSince";

		var eTag = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar")).eTag();

		var days = Calendar.getInstance();
		days.set(1994, 8, 29, 19, 43, 31);

		// If-Match: true, If-Unmodified-Since: false -> 200 OK
		var response = client.getObject(
				g -> g.bucket(bucketName).key(key).ifMatch(eTag).ifUnmodifiedSince(days.toInstant()));
		assertEquals("bar", getBody(response));
	}

	@Test
	@Tag("IfNoneMatch")
	// If-None-Match(불일치)와 If-Modified-Since(일치)를 함께 사용할 경우 ETag 조건이 우선되어 304가 반환되는지 확인
	public void testGetObjectIfNoneMatchWithIfModifiedSince() {
		var client = getClient();
		var bucketName = createBucket(client, 11);
		var key = "testGetObjectIfNoneMatchWithIfModifiedSince";

		var eTag = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar")).eTag();

		var days = Calendar.getInstance();
		days.set(1994, 8, 29, 19, 43, 31);

		// If-None-Match: false, If-Modified-Since: true -> 304 Not Modified
		var e = assertThrows(AwsServiceException.class, () -> client.getObject(
				g -> g.bucket(bucketName).key(key).ifNoneMatch(eTag).ifModifiedSince(days.toInstant())));
		assertEquals(HttpStatus.SC_NOT_MODIFIED, e.statusCode());
	}

	@Test
	@Tag("IfMatch")
	@Tag("IfNoneMatch")
	// If-Match와 If-None-Match에 동일한 ETag를 지정하면 304가 반환되는지 확인
	public void testGetObjectIfMatchAndIfNoneMatch() {
		var client = getClient();
		var bucketName = createBucket(client, 12);
		var key = "testGetObjectIfMatchAndIfNoneMatch";

		var eTag = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar")).eTag();

		var e = assertThrows(AwsServiceException.class,
				() -> client.getObject(g -> g.bucket(bucketName).key(key).ifMatch(eTag).ifNoneMatch(eTag)));
		assertEquals(HttpStatus.SC_NOT_MODIFIED, e.statusCode());
		assertNull(e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("IfMatch")
	@Tag("IfNoneMatch")
	// If-Match와 If-None-Match: * 를 함께 지정하면 304가 반환되는지 확인
	public void testGetObjectIfMatchAndIfNoneMatchAny() {
		var client = getClient();
		var bucketName = createBucket(client, 13);
		var key = "testGetObjectIfMatchAndIfNoneMatchAny";

		var eTag = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar")).eTag();

		var e = assertThrows(AwsServiceException.class,
				() -> client.getObject(g -> g.bucket(bucketName).key(key).ifMatch(eTag).ifNoneMatch("*")));
		assertEquals(HttpStatus.SC_NOT_MODIFIED, e.statusCode());
		assertNull(e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("IfMatch")
	// HeadObject에서 일치하는 If-Match 조건으로 성공 확인
	public void testHeadObjectIfMatchGood() {
		var client = getClient();
		var bucketName = createBucket(client, 14);
		var key = "testHeadObjectIfMatchGood";

		var eTag = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar")).eTag();

		var response = client.headObject(h -> h.bucket(bucketName).key(key).ifMatch(eTag));
		assertEquals(eTag, response.eTag());
	}

	@Test
	@Tag("IfMatch")
	// HeadObject에서 일치하지 않는 If-Match 조건으로 412 실패 확인
	public void testHeadObjectIfMatchFailed() {
		var client = getClient();
		var bucketName = createBucket(client, 15);
		var key = "testHeadObjectIfMatchFailed";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));

		// HEAD 응답에는 에러 본문이 없어 상태 코드만 확인
		var e = assertThrows(AwsServiceException.class, () -> client
				.headObject(h -> h.bucket(bucketName).key(key).ifMatch("ABCDEFGHIJKLMNOPQRSTUVWXYZ")));
		assertEquals(HttpStatus.SC_PRECONDITION_FAILED, e.statusCode());
	}

	@Test
	@Tag("IfNoneMatch")
	// HeadObject에서 일치하는 If-None-Match 조건으로 304 반환 확인
	public void testHeadObjectIfNoneMatchGood() {
		var client = getClient();
		var bucketName = createBucket(client, 16);
		var key = "testHeadObjectIfNoneMatchGood";

		var eTag = client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar")).eTag();

		var e = assertThrows(AwsServiceException.class,
				() -> client.headObject(h -> h.bucket(bucketName).key(key).ifNoneMatch(eTag)));
		assertEquals(HttpStatus.SC_NOT_MODIFIED, e.statusCode());
	}

	@Test
	@Tag("IfNoneMatch")
	// HeadObject에서 일치하지 않는 If-None-Match 조건으로 성공 확인
	public void testHeadObjectIfNoneMatchFailed() {
		var client = getClient();
		var bucketName = createBucket(client, 17);
		var key = "testHeadObjectIfNoneMatchFailed";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));

		var response = client
				.headObject(h -> h.bucket(bucketName).key(key).ifNoneMatch("ABCDEFGHIJKLMNOPQRSTUVWXYZ"));
		assertEquals(3, response.contentLength());
	}

	@Test
	@Tag("IfModifiedSince")
	// HeadObject에서 오브젝트 업로드 이전 시간의 If-Modified-Since 조건으로 성공 확인
	public void testHeadObjectIfModifiedSinceGood() {
		var client = getClient();
		var bucketName = createBucket(client, 18);
		var key = "testHeadObjectIfModifiedSinceGood";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));

		var days = Calendar.getInstance();
		days.set(1994, 8, 29, 19, 43, 31);
		var response = client.headObject(h -> h.bucket(bucketName).key(key).ifModifiedSince(days.toInstant()));
		assertEquals(3, response.contentLength());
	}

	@Test
	@Tag("IfModifiedSince")
	// HeadObject에서 오브젝트 업로드 이후 시간의 If-Modified-Since 조건으로 304 반환 확인
	public void testHeadObjectIfModifiedSinceFailed() {
		var client = getClient();
		var bucketName = createBucket(client, 19);
		var key = "testHeadObjectIfModifiedSinceFailed";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));

		var response = client.headObject(h -> h.bucket(bucketName).key(key));
		var after = response.lastModified().plus(1, ChronoUnit.SECONDS);

		delay(1000);

		var e = assertThrows(AwsServiceException.class,
				() -> client.headObject(h -> h.bucket(bucketName).key(key).ifModifiedSince(after)));
		assertEquals(HttpStatus.SC_NOT_MODIFIED, e.statusCode());
	}

	@Test
	@Tag("ifUnmodifiedSince")
	// HeadObject에서 오브젝트 업로드 이전 시간의 If-Unmodified-Since 조건으로 412 실패 확인
	public void testHeadObjectIfUnmodifiedSinceGood() {
		var client = getClient();
		var bucketName = createBucket(client, 20);
		var key = "testHeadObjectIfUnmodifiedSinceGood";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));

		var days = Calendar.getInstance();
		days.set(1994, 8, 29, 19, 43, 31);

		var e = assertThrows(AwsServiceException.class,
				() -> client.headObject(h -> h.bucket(bucketName).key(key).ifUnmodifiedSince(days.toInstant())));
		assertEquals(HttpStatus.SC_PRECONDITION_FAILED, e.statusCode());
	}

	@Test
	@Tag("ifUnmodifiedSince")
	// HeadObject에서 오브젝트 업로드 이후 시간의 If-Unmodified-Since 조건으로 성공 확인
	public void testHeadObjectIfUnmodifiedSinceFailed() {
		var client = getClient();
		var bucketName = createBucket(client, 21);
		var key = "testHeadObjectIfUnmodifiedSinceFailed";

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("bar"));

		var days = Calendar.getInstance();
		days.set(2100, 8, 29, 19, 43, 31);
		var response = client
				.headObject(h -> h.bucket(bucketName).key(key).ifUnmodifiedSince(days.toInstant()));
		assertEquals(3, response.contentLength());
	}

	@Test
	@Tag("Range")
	public void testRangedRequestResponseCode() {
		var key = "testRangedRequestResponseCode";
		var content = "contentData";

		var client = getClient();
		var bucketName = createBucket(client, 22);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));
		var response = client.getObject(g -> g.bucket(bucketName).key(key).range("bytes=4-7"));

		var fetchedContent = getBody(response);
		assertEquals(content.substring(4, 8), fetchedContent);
		assertEquals("bytes 4-7/11", response.response().contentRange());
	}

	@Test
	@Tag("Range")
	public void testRangedBigRequestResponseCode() {
		var key = "testRangedBigRequestResponseCode";
		var content = Utils.randomTextToLong(8 * MainData.MB);

		var client = getClient();
		var bucketName = createBucket(client, 23);

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
		var key = "testRangedRequestSkipLeadingBytesResponseCode";
		var content = "contentData";

		var client = getClient();
		var bucketName = createBucket(client, 24);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));
		var response = client.getObject(g -> g.bucket(bucketName).key(key).range("bytes=4-"));

		var fetchedContent = getBody(response);
		assertEquals(content.substring(4), fetchedContent);
		assertEquals("bytes 4-10/11", response.response().contentRange());
	}

	@Test
	@Tag("Range")
	public void testRangedRequestReturnTrailingBytesResponseCode() {
		var key = "testRangedRequestReturnTrailingBytesResponseCode";
		var content = "contentData";

		var client = getClient();
		var bucketName = createBucket(client, 25);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));
		var response = client.getObject(g -> g.bucket(bucketName).key(key).range("bytes=-7"));

		var fetchedContent = getBody(response);
		assertEquals(content.substring(content.length() - 7), fetchedContent);
		assertEquals("bytes 4-10/11", response.response().contentRange());
	}

	@Test
	@Tag("Range")
	public void testRangedRequestInvalidRange() {
		var key = "testRangedRequestInvalidRange";
		var content = "contentData";

		var client = getClient();
		var bucketName = createBucket(client, 26);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(content));
		var e = assertThrows(AwsServiceException.class,
				() -> client.getObject(g -> g.bucket(bucketName).key(key).range("bytes=40-50")));
		assertEquals(HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE, e.statusCode());
		assertEquals(MainData.INVALID_RANGE, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Range")
	public void testRangedRequestEmptyObject() {
		var key = "testRangedRequestEmptyObject";
		var content = "";

		var client = getClient();
		var bucketName = createBucket(client, 27);

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
		var bucketName = createBucket(client, 28);
		var key = "testGetObjectMany";
		var data = Utils.randomTextToLong(15 * MainData.MB);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(data));
		checkContent(bucketName, key, data, 50);
	}

	@Test
	@Tag("Get")
	public void testRangeObjectMany() {
		var client = getClient();
		var bucketName = createBucket(client, 29);
		var key = "testRangeObjectMany";
		var fileSize = 1024 * 1024 * 15;
		var data = Utils.randomTextToLong(fileSize);

		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(data));
		checkContentUsingRandomRange(bucketName, key, data, 50);
	}

	@Test
	@Tag("Range")
	@Tag("checksum")
	public void testRangeGetChecksum() {
		var client = getClient();
		var bucketName = createBucket(client, 36);
		var data = Utils.randomTextToLong(5 * MainData.MB);

		for (var checksum : CheckSum.ALL_ALGORITHMS) {
			var key = "testRangeGetChecksum/" + checksum.name();
			var response = client.putObject(
					p -> CheckSum.applyChecksum(p.bucket(bucketName).key(key), checksum, data),
					RequestBody.fromString(data));

			checksumCompare(checksum, data, response);
			checkContentUsingRandomRange(bucketName, key, data, 50);
		}
	}

	@Test
	@Tag("Header")
	public void testObjectResponseHeaders() {
		var key = "testObjectResponseHeaders";
		var client = getClient();
		var bucketName = createObjects(client, 30, key);

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
		var bucketName = createBucket(client, 31);
		var key = "testMultipartObjectRange";

		var multipartUploadData = multipartUpload(client, bucketName, key, 5 * MainData.MB, 5 * MainData.MB);

		var response = client.getObject(g -> g.bucket(bucketName).key(key).partNumber(1));
		var body = getBody(response);
		assertEquals(multipartUploadData.getBody().substring(0, 5 * MainData.MB), body);
	}

	@Test
	@Tag("Get")
	public void testGetObjectIgnore() {
		var key = "testGetObjectIgnore";
		var client = getClient();
		var bucketName = createObjects(client, 32, key);

		var response = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals(key.length(), response.response().contentLength());
	}

	@Test
	@Tag("ERROR")
	public void testGetObjectAfterDelete() {
		var client = getClient();
		var bucketName = createBucket(client, 33);
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
		var bucketName = createBucket(client, 34);
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
		var bucketName = createBucket(client, 35);
		var key = "testGetObjectDeleteMarker";
		var body = "testContent";

		// 버저닝 활성화
		client.putBucketVersioning(p -> p.bucket(bucketName)
				.versioningConfiguration(v -> v.status(BucketVersioningStatus.ENABLED)));

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
		assertEquals(MainData.METHOD_NOT_ALLOWED, e.awsErrorDetails().errorCode());

		// 실제 오브젝트 버전으로 GetObject - 성공
		var version = versions.get(0);
		var response = client.getObject(g -> g.bucket(bucketName).key(key).versionId(version.versionId()));
		var content = getBody(response);
		assertEquals(body, content);
	}

	private static final String DIGITS = "0123456789";

	/** 10바이트(0123456789) 오브젝트를 하나 넣은 버킷을 만든다. */
	private String putDigits(S3Client client, int testId, String key) {
		var bucketName = createBucket(client, testId);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString(DIGITS));
		return bucketName;
	}

	@Test
	@Tag("ERROR")
	public void testGetObjectBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(37);
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET,
				() -> client.getObject(g -> g.bucket(bucketName).key("key")));
		// HEAD 는 본문이 없어 상태 코드만 온다
		assertS3Error(HttpStatus.SC_NOT_FOUND, null, () -> client.headObject(h -> h.bucket(bucketName).key("key")));
	}

	@Test
	@Tag("ERROR")
	public void testHeadObjectNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 38);
		assertS3Error(HttpStatus.SC_NOT_FOUND, null, () -> client.headObject(h -> h.bucket(bucketName).key("key")));
	}

	@Test
	@Tag("Access")
	public void testGetObjectAccessDenied() {
		var key = "testGetObjectAccessDenied";
		var client = getClient();
		var bucketName = putDigits(client, 39, key);
		for (var otherClient : List.of(getAltClient(), getPublicClient())) {
			assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED,
					() -> otherClient.getObject(g -> g.bucket(bucketName).key(key)));
			// 목록 권한이 없으면 없는 키도 404 가 아니라 403 이다
			assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED,
					() -> otherClient.getObject(g -> g.bucket(bucketName).key("not_exist")));
			assertS3Error(HttpStatus.SC_FORBIDDEN, null,
					() -> otherClient.headObject(h -> h.bucket(bucketName).key(key)));
		}
	}

	@Test
	@Tag("Range")
	public void testRangedRequestIgnored() {
		var key = "testRangedRequestIgnored";
		var client = getClient();
		var bucketName = putDigits(client, 40, key);
		// 여러 구간, 잘못된 형식, 다른 단위, 뒤집힌 구간은 무시하고 전체를 200 으로 돌려준다
		for (var value : List.of("bytes=0-1,3-4", "bytes=abc", "items=0-1", "bytes=5-2")) {
			var response = client.getObject(g -> g.bucket(bucketName).key(key).range(value));
			assertEquals(HttpStatus.SC_OK, response.response().sdkHttpResponse().statusCode());
			assertNull(response.response().contentRange());
			assertEquals(DIGITS, getBody(response));
		}
	}

	@Test
	@Tag("Range")
	public void testRangedRequestBeyondEnd() {
		var key = "testRangedRequestBeyondEnd";
		var client = getClient();
		var bucketName = putDigits(client, 41, key);
		// 끝이 크기를 넘으면 마지막 바이트까지로 잘린다
		var response = client.getObject(g -> g.bucket(bucketName).key(key).range("bytes=5-100"));
		assertEquals(HttpStatus.SC_PARTIAL_CONTENT, response.response().sdkHttpResponse().statusCode());
		assertEquals("bytes 5-9/10", response.response().contentRange());
		assertEquals("56789", getBody(response));

		// 뒤에서부터의 길이가 크기를 넘으면 전체를 206 으로 돌려준다
		response = client.getObject(g -> g.bucket(bucketName).key(key).range("bytes=-100"));
		assertEquals(HttpStatus.SC_PARTIAL_CONTENT, response.response().sdkHttpResponse().statusCode());
		assertEquals("bytes 0-9/10", response.response().contentRange());
		assertEquals(DIGITS, getBody(response));

		// 시작이 크기와 같거나, 뒤에서 0바이트를 요청하면 416
		for (var value : List.of("bytes=10-20", "bytes=-0")) {
			assertS3Error(HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE, MainData.INVALID_RANGE,
					() -> client.getObject(g -> g.bucket(bucketName).key(key).range(value)));
		}
	}

	@Test
	@Tag("Range")
	public void testHeadObjectRange() {
		var key = "testHeadObjectRange";
		var client = getClient();
		var bucketName = putDigits(client, 42, key);
		var response = client.headObject(h -> h.bucket(bucketName).key(key).range("bytes=0-4"));
		assertEquals(HttpStatus.SC_PARTIAL_CONTENT, response.sdkHttpResponse().statusCode());
		assertEquals(5, response.contentLength());
		assertEquals("bytes 0-4/10", response.contentRange());
	}

	@Test
	@Tag("PartNumber")
	public void testGetObjectPartNumber() {
		var key = "testGetObjectPartNumber";
		var client = getClient();
		var bucketName = putDigits(client, 43, key);
		// 멀티파트가 아닌 오브젝트는 1번 파트가 전체다
		var response = client.getObject(g -> g.bucket(bucketName).key(key).partNumber(1));
		assertEquals(HttpStatus.SC_PARTIAL_CONTENT, response.response().sdkHttpResponse().statusCode());
		assertEquals("bytes 0-9/10", response.response().contentRange());
		assertNull(response.response().partsCount());
		assertEquals(DIGITS, getBody(response));

		var headResponse = client.headObject(h -> h.bucket(bucketName).key(key).partNumber(1));
		assertEquals(HttpStatus.SC_PARTIAL_CONTENT, headResponse.sdkHttpResponse().statusCode());
		assertEquals(10, headResponse.contentLength());

		assertS3Error(HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE, MainData.INVALID_PART_NUMBER,
				() -> client.getObject(g -> g.bucket(bucketName).key(key).partNumber(2)));
	}

	@Test
	@Tag("PartNumber")
	public void testGetObjectPartNumberInvalid() {
		var key = "testGetObjectPartNumberInvalid";
		var client = getClient();
		var bucketName = putDigits(client, 44, key);
		// 파트 번호는 1~10000
		for (var partNumber : List.of(0, 10001)) {
			assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT,
					() -> client.getObject(g -> g.bucket(bucketName).key(key).partNumber(partNumber)));
		}
		// Range 와 partNumber 는 함께 쓸 수 없다
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_REQUEST,
				() -> client.getObject(g -> g.bucket(bucketName).key(key).partNumber(1).range("bytes=0-1")));
	}

	@Test
	@Tag("Version")
	public void testGetObjectVersionIdUnversioned() {
		var key = "testGetObjectVersionIdUnversioned";
		var client = getClient();
		var bucketName = putDigits(client, 45, key);
		// 버저닝을 쓰지 않는 버킷의 오브젝트는 null 버전이다
		assertEquals(DIGITS, getBody(client.getObject(g -> g.bucket(bucketName).key(key).versionId("null"))));
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT,
				() -> client.getObject(g -> g.bucket(bucketName).key(key).versionId("invalid")));
		assertS3Error(HttpStatus.SC_BAD_REQUEST, null,
				() -> client.headObject(h -> h.bucket(bucketName).key(key).versionId("invalid")));
	}

	@Test
	@Tag("Version")
	public void testGetObjectNullVersionNotExist() {
		var key = "testGetObjectNullVersionNotExist";
		var client = getClient();
		var bucketName = createBucket(client, 46);
		checkConfigureVersioningRetry(bucketName, BucketVersioningStatus.ENABLED);
		client.putObject(p -> p.bucket(bucketName).key(key), RequestBody.fromString("content"));
		// 버저닝을 켠 뒤에 만든 오브젝트에는 null 버전이 없다
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_VERSION,
				() -> client.getObject(g -> g.bucket(bucketName).key(key).versionId("null")));
		assertS3Error(HttpStatus.SC_NOT_FOUND, null,
				() -> client.headObject(h -> h.bucket(bucketName).key(key).versionId("null")));
	}

	@Test
	@Tag("Header")
	public void testGetObjectResponseHeadersAnonymous() {
		var key = "testGetObjectResponseHeadersAnonymous";
		var client = getClient();
		var bucketName = putDigits(client, 47, key);
		// 익명 요청에는 response-* 재정의를 쓸 수 없다
		var publicClient = getPublicClient();
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_REQUEST,
				() -> publicClient.getObject(g -> g.bucket(bucketName).key(key).responseContentType("text/html")));
	}

	@Test
	@Tag("Header")
	public void testHeadObjectResponseHeaders() {
		var key = "testHeadObjectResponseHeaders";
		var client = getClient();
		var bucketName = putDigits(client, 48, key);
		var response = client.headObject(h -> h.bucket(bucketName).key(key)
				.responseContentType("text/html")
				.responseContentDisposition("inline")
				.responseCacheControl("max-age=1")
				.responseContentEncoding("identity")
				.responseContentLanguage("en"));
		assertEquals("text/html", response.contentType());
		assertEquals("inline", response.contentDisposition());
		assertEquals("max-age=1", response.cacheControl());
		assertEquals("identity", response.contentEncoding());
		assertEquals("en", response.contentLanguage());
	}

	@Test
	@Tag("Header")
	public void testGetObjectDefaultHeaders() {
		var key = "testGetObjectDefaultHeaders";
		var client = getClient();
		var bucketName = putDigits(client, 49, key);
		var eTag = "\"781e5e245d69b566979b86e28d23f2c7\"";
		// (SDK 가 Content-Type 을 항상 붙여 올리므로 기본값 binary/octet-stream 은 확인하지 않는다)
		var getResponse = client.getObject(g -> g.bucket(bucketName).key(key));
		assertEquals("bytes", getResponse.response().acceptRanges());
		assertEquals(10, getResponse.response().contentLength());
		assertEquals(eTag, getResponse.response().eTag());
		assertTrue(getResponse.response().lastModified() != null);
		getBody(getResponse);

		var headResponse = client.headObject(h -> h.bucket(bucketName).key(key));
		assertEquals("bytes", headResponse.acceptRanges());
		assertEquals(10, headResponse.contentLength());
		assertEquals(eTag, headResponse.eTag());
		assertTrue(headResponse.lastModified() != null);
	}

	@Test
	@Tag("IfMatch")
	public void testGetObjectIfMatchWildcard() {
		var key = "testGetObjectIfMatchWildcard";
		var client = getClient();
		var bucketName = putDigits(client, 50, key);
		// If-Match: * 는 오브젝트가 있으면 통과, If-None-Match: * 는 304
		assertEquals(DIGITS, getBody(client.getObject(g -> g.bucket(bucketName).key(key).ifMatch("*"))));
		assertS3Error(HttpStatus.SC_NOT_MODIFIED, null,
				() -> client.getObject(g -> g.bucket(bucketName).key(key).ifNoneMatch("*")));
	}
}
