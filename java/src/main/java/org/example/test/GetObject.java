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

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ResponseHeaderOverrides;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;

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
	public void testObjectReadNotExist() {
		var client = getClient();
		var bucketName = createBucket(client);

		var e = assertThrows(AmazonServiceException.class, () -> client.getObject(bucketName, "bar"));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.getErrorCode());
	}

	@Test
	@Tag("IfMatch")
	public void testGetObjectIfMatchGood() {
		var key = "foo";
		var client = getClient();
		var bucketName = createBucket(client);

		var putResponse = client.putObject(bucketName, key, "bar");
		var eTag = putResponse.getETag();

		var getResponse = client.getObject(new GetObjectRequest(bucketName, key).withMatchingETagConstraint(eTag));
		var body = getBody(getResponse.getObjectContent());
		assertEquals("bar", body);
	}

	@Test
	@Tag("IfMatch")
	public void testGetObjectIfMatchFailed() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";

		client.putObject(bucketName, key, "bar");
		var response = client.getObject(
				new GetObjectRequest(bucketName, key).withMatchingETagConstraint("ABCDEFGHIJKLMNOPQRSTUVWXYZ"));
		assertNull(response);
	}

	@Test
	@Tag("IfNoneMatch")
	public void testGetObjectIfNoneMatchGood() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";

		var putResponse = client.putObject(bucketName, key, "bar");
		var eTag = putResponse.getETag();

		var response = client.getObject(new GetObjectRequest(bucketName, key).withNonmatchingETagConstraint(eTag));
		assertNull(response);
	}

	@Test
	@Tag("IfNoneMatch")
	public void testGetObjectIfNoneMatchFailed() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";

		client.putObject(bucketName, key, "bar");

		var getResponse = client
				.getObject(new GetObjectRequest(bucketName, key)
						.withNonmatchingETagConstraint("ABCDEFGHIJKLMNOPQRSTUVWXYZ"));
		var body = getBody(getResponse.getObjectContent());
		assertEquals("bar", body);
	}

	@Test
	@Tag("IfModifiedSince")
	public void testGetObjectIfModifiedSinceGood() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";

		client.putObject(bucketName, key, "bar");

		var days = Calendar.getInstance();
		days.set(1994, 8, 29, 19, 43, 31);
		var response = client
				.getObject(new GetObjectRequest(bucketName, key).withModifiedSinceConstraint(days.getTime()));
		var body = getBody(response.getObjectContent());
		assertEquals("bar", body);
	}

	@Test
	@Tag("IfModifiedSince")
	public void testGetObjectIfModifiedSinceFailed() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";

		client.putObject(bucketName, key, "bar");

		var response = client.getObject(bucketName, key);

		var after = Calendar.getInstance();
		after.setTime(response.getObjectMetadata().getLastModified());
		after.add(Calendar.SECOND, 1);

		delay(1000);

		response = client
				.getObject(new GetObjectRequest(bucketName, key).withModifiedSinceConstraint(after.getTime()));
		assertNull(response);
	}

	@Test
	@Tag("IfUnmodifiedSince")
	public void testGetObjectIfUnmodifiedSinceGood() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";

		client.putObject(bucketName, key, "bar");

		var days = Calendar.getInstance();
		days.set(1994, 8, 29, 19, 43, 31);

		var response = client
				.getObject(new GetObjectRequest(bucketName, key).withUnmodifiedSinceConstraint(days.getTime()));
		assertNull(response);
	}

	@Test
	@Tag("IfUnmodifiedSince")
	public void testGetObjectIfUnmodifiedSinceFailed() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";

		client.putObject(bucketName, key, "bar");

		var days = Calendar.getInstance();
		days.set(2100, 8, 29, 19, 43, 31);
		var response = client
				.getObject(new GetObjectRequest(bucketName, key).withUnmodifiedSinceConstraint(days.getTime()));
		var body = getBody(response.getObjectContent());
		assertEquals("bar", body);
	}

	@Test
	@Tag("Range")
	public void testRangedRequestResponseCode() {
		var key = "obj";
		var content = "contentData";

		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(bucketName, key, content);
		var response = client.getObject(new GetObjectRequest(bucketName, key).withRange(4, 7));

		var fetchedContent = getBody(response.getObjectContent());
		assertEquals(content.substring(4, 8), fetchedContent);
		assertEquals("bytes 4-7/11", response.getObjectMetadata().getRawMetadataValue(Headers.CONTENT_RANGE));
	}

	@Test
	@Tag("Range")
	public void testRangedBigRequestResponseCode() {
		var key = "obj";
		var content = Utils.randomTextToLong(8 * MainData.MB);

		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(bucketName, key, content);
		var response = client.getObject(new GetObjectRequest(bucketName, key).withRange(3145728, 5242880));

		var fetchedContent = getBody(response.getObjectContent());
		assertTrue(content.substring(3145728, 5242881).equals(fetchedContent), MainData.NOT_MATCHED);
		assertEquals("bytes 3145728-5242880/8388608",
				response.getObjectMetadata().getRawMetadataValue(Headers.CONTENT_RANGE));
	}

	@Test
	@Tag("Range")
	public void testRangedRequestSkipLeadingBytesResponseCode() {
		var key = "obj";
		var content = "contentData";

		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(bucketName, key, content);
		var response = client.getObject(new GetObjectRequest(bucketName, key).withRange(4));

		var fetchedContent = getBody(response.getObjectContent());
		assertEquals(content.substring(4), fetchedContent);
		assertEquals("bytes 4-10/11", response.getObjectMetadata().getRawMetadataValue(Headers.CONTENT_RANGE));
	}

	@Test
	@Tag("Range")
	public void testRangedRequestReturnTrailingBytesResponseCode() {
		var key = "obj";
		var content = "contentData";

		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(bucketName, key, content);
		var response = client.getObject(new GetObjectRequest(bucketName, key).withRange(4));

		var fetchedContent = getBody(response.getObjectContent());
		assertEquals(content.substring(4), fetchedContent);
		assertEquals("bytes 4-10/11", response.getObjectMetadata().getRawMetadataValue(Headers.CONTENT_RANGE));
	}

	@Test
	@Tag("Range")
	public void testRangedRequestInvalidRange() {
		var key = "obj";
		var content = "contentData";

		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(bucketName, key, content);
		var e = assertThrows(AmazonServiceException.class,
				() -> client.getObject(new GetObjectRequest(bucketName, key).withRange(40, 50)));
		assertEquals(HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE, e.getStatusCode());
		assertEquals(MainData.INVALID_RANGE, e.getErrorCode());
	}

	@Test
	@Tag("Range")
	public void testRangedRequestEmptyObject() {
		var key = "obj";
		var content = "";

		var client = getClient();
		var bucketName = createBucket(client);

		client.putObject(bucketName, key, content);
		var e = assertThrows(AmazonServiceException.class,
				() -> client.getObject(new GetObjectRequest(bucketName, key).withRange(40, 50)));
		assertEquals(HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE, e.getStatusCode());
		assertEquals(MainData.INVALID_RANGE, e.getErrorCode());
	}

	@Test
	@Tag("Get")
	public void testGetObjectMany() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "foo";
		var data = Utils.randomTextToLong(15 * MainData.MB);

		client.putObject(bucketName, key, data);
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

		client.putObject(bucketName, key, data);
		checkContentUsingRandomRange(bucketName, key, data, 50);
	}

	@Test
	@Tag("Header")
	public void testObjectResponseHeaders() {
		var key = "testObjectResponseHeaders";
		var client = getClient();
		var bucketName = createObjects(client, key);

		var date = new Date();
		SimpleDateFormat rfc822format = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss 'GMT'", Locale.ENGLISH);
		String strDate = rfc822format.format(date);

		var response = client.getObject(new GetObjectRequest(bucketName, key).withResponseHeaders(
				new ResponseHeaderOverrides()
						.withCacheControl("no-cache")
						.withContentDisposition("bla")
						.withContentEncoding("aaa")
						.withContentLanguage("esperanto")
						.withContentType("foo/bar")
						.withExpires(strDate)));

		assertEquals("no-cache", response.getObjectMetadata().getCacheControl());
		assertEquals("bla", response.getObjectMetadata().getContentDisposition());
		assertEquals("aaa", response.getObjectMetadata().getContentEncoding());
		assertEquals("esperanto", response.getObjectMetadata().getContentLanguage());
		assertEquals("foo/bar", response.getObjectMetadata().getContentType());
	}

	@Test
	@Tag("Get")
	public void testGetObjectIgnore() {
		var key = "testObjectIgnore";
		var client = getClient();
		var bucketName = createObjects(client, key);

		var response = client.getObject(new GetObjectRequest(bucketName, key));
		assertEquals(key.length(), response.getObjectMetadata().getContentLength());
	}

	@Test
	@Tag("ERROR")
	public void testGetObjectAfterDelete() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testGetObjectAfterDelete";
		var body = "testContent";

		// 오브젝트 업로드
		client.putObject(bucketName, key, body);

		// GetObject로 업로드 확인
		var getResponse = client.getObject(bucketName, key);
		var fetchedBody = getBody(getResponse.getObjectContent());
		assertEquals(body, fetchedBody);
		assertEquals(body.length(), getResponse.getObjectMetadata().getContentLength());

		// 오브젝트 삭제
		client.deleteObject(bucketName, key);

		// 삭제 후 GetObject 실패 확인
		var e = assertThrows(AmazonServiceException.class, () -> client.getObject(bucketName, key));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.getErrorCode());
	}

	@Test
	@Tag("ERROR")
	public void testGetObjectAfterDeleteVersioning() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testGetObjectAfterDeleteVersioning";
		var body = "testContent";

		// 버저닝 활성화
		client.setBucketVersioningConfiguration(
				new SetBucketVersioningConfigurationRequest(bucketName,
						new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED)));

		// 오브젝트 업로드
		client.putObject(bucketName, key, body);

		// GetObject로 업로드 확인
		var getResponse = client.getObject(bucketName, key);
		var fetchedBody = getBody(getResponse.getObjectContent());
		assertEquals(body, fetchedBody);
		assertEquals(body.length(), getResponse.getObjectMetadata().getContentLength());

		// 오브젝트 삭제 (DeleteMarker 생성)
		client.deleteObject(bucketName, key);

		// 삭제 후 GetObject 실패 확인
		var e = assertThrows(AmazonServiceException.class, () -> client.getObject(bucketName, key));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.getErrorCode());
	}

	@Test
	@Tag("Versioning")
	public void testGetObjectDeleteMarker() {
		var client = getClient();
		var bucketName = createBucket(client);
		var key = "testGetObjectDeleteMarker";
		var body = "testContent";

		// 버저닝 활성화
		client.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(bucketName,
				new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED)));

		// 오브젝트 업로드
		client.putObject(bucketName, key, body);

		// GetObject로 업로드 확인
		var getResponse = client.getObject(bucketName, key);
		var fetchedBody = getBody(getResponse.getObjectContent());
		assertEquals(body, fetchedBody);

		// 오브젝트 삭제 (DeleteMarker 생성)
		client.deleteObject(bucketName, key);

		// ListObjectVersions로 버전 목록 조회
		var listResponse = client.listVersions(bucketName, "");
		var versions = listResponse.getVersionSummaries();

		// 2개의 버전이 있어야 함 (DeleteMarker + 실제 오브젝트)
		assertEquals(2, versions.size());

		// 각 버전에 대해 순차적으로 GetObject 시도
		for (var version : versions) {
			if (version.isDeleteMarker()) {
				// DeleteMarker는 GetObject 실패
				var e = assertThrows(AmazonServiceException.class, () -> client
						.getObject(new GetObjectRequest(bucketName, key).withVersionId(version.getVersionId())));
				assertEquals(HttpStatus.SC_METHOD_NOT_ALLOWED, e.getStatusCode());
				assertEquals(MainData.METHOD_NOT_ALLOWED, e.getErrorCode());
			} else {
				// 실제 오브젝트 버전은 GetObject 성공
				var response = client
						.getObject(new GetObjectRequest(bucketName, key).withVersionId(version.getVersionId()));
				var content = getBody(response.getObjectContent());
				assertEquals(body, content);
			}
		}
	}
}
