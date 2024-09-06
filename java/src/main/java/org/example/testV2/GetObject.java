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

import java.time.temporal.ChronoUnit;
import java.util.Calendar;

import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;

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
		var statusCode = e.statusCode();
		var errorCode = e.awsErrorDetails().errorCode();

		assertEquals(404, statusCode);
		assertEquals(MainData.NO_SUCH_KEY, errorCode);
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

		assertEquals(412, e.statusCode());
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

		assertEquals(304, e.statusCode());
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

		assertEquals(304, e.statusCode());
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

		assertEquals(412, e.statusCode());
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
		var statusCode = e.statusCode();
		var errorCode = e.awsErrorDetails().errorCode();
		assertEquals(416, statusCode);
		assertEquals(MainData.INVALID_RANGE, errorCode);
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
		var statusCode = e.statusCode();
		var errorCode = e.awsErrorDetails().errorCode();
		assertEquals(416, statusCode);
		assertEquals(MainData.INVALID_RANGE, errorCode);
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
}
