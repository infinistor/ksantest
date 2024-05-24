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
		System.out.println("GetObject Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("GetObject End");
	}

	@Test
	@Tag("ERROR")
	// 버킷에 존재하지 않는 오브젝트 다운로드를 할 경우 실패 확인
	public void objectReadNotExist() {
		var bucketName = getNewBucket();
		var client = getClient();

		var e = assertThrows(AwsServiceException.class,
				() -> client.getObject(g -> g.bucket(bucketName).key("foo").build()));
		var statusCode = e.statusCode();
		var errorCode = e.getMessage();

		assertEquals(404, statusCode);
		assertEquals(MainData.NoSuchKey, errorCode);
	}

	@Test
	@Tag("IfMatch")
	// 존재하는 오브젝트 이름과 ETag 값으로 오브젝트를 가져오는지 확인
	public void testGetObjectIfMatchGood() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("bar"));
		var eTag = putResponse.eTag();

		var getResponse = client.getObject(g -> g.bucket(bucketName).key(key).ifMatch(eTag).build());
		var body = getBody(getResponse);
		assertEquals("bar", body);
	}

	@Test
	@Tag("IfMatch")
	// 오브젝트와 일치하지 않는 ETag 값을 설정하여 오브젝트 조회 실패 확인
	public void testGetObjectIfMatchFailed() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("bar"));
		var response = client
				.getObject(g -> g.bucket(bucketName).key(key).ifMatch("ABCDEFGHIJKLMNOPQRSTUVWXYZ").build());
		assertNull(response);
	}

	@Test
	@Tag("IfNoneMatch")
	// 오브젝트와 일치하는 ETag 값을 IfsNoneMatch에 설정하여 오브젝트 조회 실패
	public void testGetObjectIfNoneMatchGood() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		var putResponse = client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("bar"));
		var eTag = putResponse.eTag();

		var response = client.getObject(g -> g.bucket(bucketName).key(key).ifNoneMatch(eTag).build());
		assertNull(response);
	}

	@Test
	@Tag("IfNoneMatch")
	// 오브젝트와 일치하지 않는 ETag 값을 IfsNoneMatch에 설정하여 오브젝트 조회 성공
	public void testGetObjectIfNoneMatchFailed() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("bar"));

		var getResponse = client
				.getObject(g -> g.bucket(bucketName).key(key).ifNoneMatch("ABCDEFGHIJKLMNOPQRSTUVWXYZ").build());
		var body = getBody(getResponse);
		assertEquals("bar", body);
	}

	@Test
	@Tag("IfModifiedSince")
	// [지정일을 오브젝트 업로드 시간 이전으로 설정] 지정일(ifModifiedSince)보다 이후에 수정된 오브젝트를 조회 성공
	public void testGetObjectIfModifiedSinceGood() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("bar"));

		var days = Calendar.getInstance();
		days.set(1994, 8, 29, 19, 43, 31);
		var response = client
				.getObject(g -> g.bucket(bucketName).key(key).ifModifiedSince(days.toInstant()).build());
		var body = getBody(response);
		assertEquals("bar", body);
	}

	@Test
	@Tag("IfModifiedSince")
	// [지정일을 오브젝트 업로드 시간 이후로 설정] 지정일(ifModifiedSince)보다 이전에 수정된 오브젝트 조회 실패
	public void testGetObjectIfModifiedSinceFailed() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("bar"));
		var response = client.getObject(g -> g.bucket(bucketName).key(key).build());
		var after = response.response().lastModified().plus(1, ChronoUnit.SECONDS);

		delay(1000);

		response = client
				.getObject(g -> g.bucket(bucketName).key(key).ifUnmodifiedSince(after).build());
		assertNull(response);
	}

	@Test
	@Tag("ifUnmodifiedSince")
	// [지정일을 오브젝트 업로드 시간 이전으로 설정] 지정일(ifUnmodifiedSince) 이후 수정되지 않은 오브젝트 조회 실패
	public void testGetObjectIfUnmodifiedSinceGood() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("bar"));

		var days = Calendar.getInstance();
		days.set(1994, 8, 29, 19, 43, 31);

		var response = client
				.getObject(g -> g.bucket(bucketName).key(key).ifUnmodifiedSince(days.toInstant()).build());
		assertNull(response);
	}

	@Test
	@Tag("ifUnmodifiedSince")
	// [지정일을 오브젝트 업로드 시간 이후으로 설정] 지정일(ifUnmodifiedSince) 이후 수정되지 않은 오브젝트 조회 성공
	public void testGetObjectIfUnmodifiedSinceFailed() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("bar"));

		var days = Calendar.getInstance();
		days.set(2100, 8, 29, 19, 43, 31);
		var response = client
				.getObject(g -> g.bucket(bucketName).key(key).ifUnmodifiedSince(days.toInstant()).build());
		var body = getBody(response);
		assertEquals("bar", body);
	}

	@Test
	@Tag("Range")
	// 지정한 범위로 오브젝트 다운로드가 가능한지 확인
	public void testRangedRequestResponseCode() {
		var key = "obj";
		var content = "content";

		var bucketName = getNewBucket();
		var client = getClient();

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(content));
		var response = client.getObject(g -> g.bucket(bucketName).key(key).range("bytes=4-7").build());

		var fetchedContent = getBody(response);
		assertEquals(content.substring(4, 8), fetchedContent);
		assertEquals("bytes 4-7/11", response.response().contentRange());
	}

	@Test
	@Tag("Range")
	// 지정한 범위로 대용량인 오브젝트 다운로드가 가능한지 확인
	public void testRangedBigRequestResponseCode() {
		var key = "obj";
		var content = Utils.randomTextToLong(8 * MainData.MB);

		var bucketName = getNewBucket();
		var client = getClient();

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(content));
		var response = client.getObject(g -> g.bucket(bucketName).key(key).range("bytes=3145728-5242880").build());

		var fetchedContent = getBody(response);
		assertTrue(content.substring(3145728, 5242881).equals(fetchedContent), MainData.NOT_MATCHED);
		assertEquals("bytes 3145728-5242880/8388608",
				response.response().contentRange());
	}

	@Test
	@Tag("Range")
	// 특정지점부터 끝까지 오브젝트 다운로드 가능한지 확인
	public void testRangedRequestSkipLeadingBytesResponseCode() {
		var key = "obj";
		var content = "content";

		var bucketName = getNewBucket();
		var client = getClient();

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(content));
		var response = client.getObject(g -> g.bucket(bucketName).key(key).range("bytes=4-").build());

		var fetchedContent = getBody(response);
		assertEquals(content.substring(4), fetchedContent);
		assertEquals("bytes 4-10/11", response.response().contentRange());
	}

	@Test
	@Tag("Range")
	// 끝에서 부터 특정 길이까지 오브젝트 다운로드 가능한지 확인
	public void testRangedRequestReturnTrailingBytesResponseCode() {
		var key = "obj";
		var content = "content";

		var bucketName = getNewBucket();
		var client = getClient();

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(content));
		var response = client.getObject(g -> g.bucket(bucketName).key(key).range("bytes=-7").build());

		var fetchedContent = getBody(response);
		assertEquals(content.substring(content.length() - 7), fetchedContent);
		assertEquals("bytes 4-10/11", response.response().contentRange());
	}

	@Test
	@Tag("Range")
	// 오브젝트의 크기를 초과한 범위를 설정하여 다운로드 할경우 실패 확인
	public void testRangedRequestInvalidRange() {
		var key = "obj";
		var content = "content";

		var bucketName = getNewBucket();
		var client = getClient();

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(content));
		var e = assertThrows(AwsServiceException.class,
				() -> client.getObject(g -> g.bucket(bucketName).key(key).range("bytes=40-50").build()));
		var statusCode = e.statusCode();
		var errorCode = e.getMessage();
		assertEquals(416, statusCode);
		assertEquals(MainData.InvalidRange, errorCode);
	}

	@Test
	@Tag("Range")
	// 비어있는 오브젝트를 범위를 지정하여 다운로드 실패 확인
	public void testRangedRequestEmptyObject() {
		var key = "obj";
		var content = "";

		var bucketName = getNewBucket();
		var client = getClient();

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(content));
		var e = assertThrows(AwsServiceException.class,
				() -> client.getObject(g -> g.bucket(bucketName).key(key).range("bytes=40-50").build()));
		var statusCode = e.statusCode();
		var errorCode = e.getMessage();
		assertEquals(416, statusCode);
		assertEquals(MainData.InvalidRange, errorCode);
	}

	@Test
	@Tag("Get")
	// 같은 오브젝트를 여러번 반복하여 다운로드 성공 확인
	public void testGetObjectMany() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";
		var data = Utils.randomTextToLong(15 * MainData.MB);

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(data));
		checkContent(bucketName, key, data, 50);
	}

	@Test
	@Tag("Get")
	// 같은 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
	public void testRangeObjectMany() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";
		var fileSize = 1024 * 1024 * 15;
		var data = Utils.randomTextToLong(fileSize);

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString(data));
		checkContentUsingRandomRange(bucketName, key, data, 50);
	}

	@Test
	@Tag("Restore")
	// 오브젝트 복구 명령이 성공하는지 확인
	public void testRestoreObject() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		client.putObject(p -> p.bucket(bucketName).key(key).build(), RequestBody.fromString("bar"));

		client.restoreObject(r -> r.bucket(bucketName).key(key).build());
	}
}
