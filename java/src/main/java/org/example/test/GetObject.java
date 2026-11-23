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
import java.util.List;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;

public class GetObject extends TestBase {
	@Test
	@Tag("ERROR")
	public void testObjectReadNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 1);

		var e = assertThrows(AmazonServiceException.class,
				() -> client.getObject(bucketName, "testObjectReadNotExist"));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_KEY, e.getErrorCode());
	}

	@Test
	@Tag("IfMatch")
	public void testGetObjectIfMatchGood() {
		var key = "testGetObjectIfMatchGood";
		var client = getClient();
		var bucketName = createBucket(client, 2);

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
		var bucketName = createBucket(client, 3);
		var key = "testGetObjectIfMatchFailed";

		client.putObject(bucketName, key, "bar");
		var response = client.getObject(
				new GetObjectRequest(bucketName, key).withMatchingETagConstraint("ABCDEFGHIJKLMNOPQRSTUVWXYZ"));
		assertNull(response);
	}

	@Test
	@Tag("IfNoneMatch")
	public void testGetObjectIfNoneMatchGood() {
		var client = getClient();
		var bucketName = createBucket(client, 4);
		var key = "testGetObjectIfNoneMatchGood";

		var putResponse = client.putObject(bucketName, key, "bar");
		var eTag = putResponse.getETag();

		var response = client.getObject(new GetObjectRequest(bucketName, key).withNonmatchingETagConstraint(eTag));
		assertNull(response);
	}

	@Test
	@Tag("IfNoneMatch")
	public void testGetObjectIfNoneMatchFailed() {
		var client = getClient();
		var bucketName = createBucket(client, 5);
		var key = "testGetObjectIfNoneMatchFailed";

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
		var bucketName = createBucket(client, 6);
		var key = "testGetObjectIfModifiedSinceGood";

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
		var bucketName = createBucket(client, 7);
		var key = "testGetObjectIfModifiedSinceFailed";

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
		var bucketName = createBucket(client, 8);
		var key = "testGetObjectIfUnmodifiedSinceGood";

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
		var bucketName = createBucket(client, 9);
		var key = "testGetObjectIfUnmodifiedSinceFailed";

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
		var key = "testRangedRequestResponseCode";
		var content = "contentData";

		var client = getClient();
		var bucketName = createBucket(client, 10);

		client.putObject(bucketName, key, content);
		var response = client.getObject(new GetObjectRequest(bucketName, key).withRange(4, 7));

		var fetchedContent = getBody(response.getObjectContent());
		assertEquals(content.substring(4, 8), fetchedContent);
		assertEquals("bytes 4-7/11", response.getObjectMetadata().getRawMetadataValue(Headers.CONTENT_RANGE));
	}

	@Test
	@Tag("Range")
	public void testRangedBigRequestResponseCode() {
		var key = "testRangedBigRequestResponseCode";
		var content = Utils.randomTextToLong(8 * MainData.MB);

		var client = getClient();
		var bucketName = createBucket(client, 11);

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
		var key = "testRangedRequestSkipLeadingBytesResponseCode";
		var content = "contentData";

		var client = getClient();
		var bucketName = createBucket(client, 12);

		client.putObject(bucketName, key, content);
		var response = client.getObject(new GetObjectRequest(bucketName, key).withRange(4));

		var fetchedContent = getBody(response.getObjectContent());
		assertEquals(content.substring(4), fetchedContent);
		assertEquals("bytes 4-10/11", response.getObjectMetadata().getRawMetadataValue(Headers.CONTENT_RANGE));
	}

	@Test
	@Tag("Range")
	public void testRangedRequestInvalidRange() {
		var key = "testRangedRequestInvalidRange";
		var content = "contentData";

		var client = getClient();
		var bucketName = createBucket(client, 13);

		client.putObject(bucketName, key, content);
		var e = assertThrows(AmazonServiceException.class,
				() -> client.getObject(new GetObjectRequest(bucketName, key).withRange(40, 50)));
		assertEquals(HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE, e.getStatusCode());
		assertEquals(MainData.INVALID_RANGE, e.getErrorCode());
	}

	@Test
	@Tag("Range")
	public void testRangedRequestEmptyObject() {
		var key = "testRangedRequestEmptyObject";
		var content = "";

		var client = getClient();
		var bucketName = createBucket(client, 14);

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
		var bucketName = createBucket(client, 15);
		var key = "testGetObjectMany";
		var data = Utils.randomTextToLong(15 * MainData.MB);

		client.putObject(bucketName, key, data);
		checkContent(bucketName, key, data, 50);
	}

	@Test
	@Tag("Get")
	public void testRangeObjectMany() {
		var client = getClient();
		var bucketName = createBucket(client, 16);
		var key = "testRangeObjectMany";
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
		var bucketName = createObjects(client, 17, key);

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
		var key = "testGetObjectIgnore";
		var client = getClient();
		var bucketName = createObjects(client, 18, key);

		var response = client.getObject(new GetObjectRequest(bucketName, key));
		assertEquals(key.length(), response.getObjectMetadata().getContentLength());
	}

	@Test
	@Tag("ERROR")
	public void testGetObjectAfterDelete() {
		var client = getClient();
		var bucketName = createBucket(client, 19);
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
		var bucketName = createBucket(client, 20);
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
		var bucketName = createBucket(client, 21);
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

	private static final String DIGITS = "0123456789";

	/** 10바이트(0123456789) 오브젝트를 하나 넣은 버킷을 만든다. */
	private String putDigits(AmazonS3 client, int testId, String key) {
		var bucketName = createBucket(client, testId);
		client.putObject(bucketName, key, DIGITS);
		return bucketName;
	}

	@Test
	@Tag("ERROR")
	public void testGetObjectBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(37);
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_BUCKET, () -> client.getObject(bucketName, "key"));
		// HEAD 는 본문이 없어 상태 코드만 온다
		assertS3Error(HttpStatus.SC_NOT_FOUND, null, () -> client.getObjectMetadata(bucketName, "key"));
	}

	@Test
	@Tag("ERROR")
	public void testHeadObjectNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 38);
		assertS3Error(HttpStatus.SC_NOT_FOUND, null, () -> client.getObjectMetadata(bucketName, "key"));
	}

	@Test
	@Tag("Access")
	public void testGetObjectAccessDenied() {
		var key = "testGetObjectAccessDenied";
		var client = getClient();
		var bucketName = putDigits(client, 39, key);
		for (var otherClient : List.of(getAltClient(), getPublicClient())) {
			assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED,
					() -> otherClient.getObject(bucketName, key));
			// 목록 권한이 없으면 없는 키도 404 가 아니라 403 이다
			assertS3Error(HttpStatus.SC_FORBIDDEN, MainData.ACCESS_DENIED,
					() -> otherClient.getObject(bucketName, "not_exist"));
			assertS3Error(HttpStatus.SC_FORBIDDEN, null, () -> otherClient.getObjectMetadata(bucketName, key));
		}
	}

	@Test
	@Tag("Range")
	public void testRangedRequestBeyondEnd() {
		var key = "testRangedRequestBeyondEnd";
		var client = getClient();
		var bucketName = putDigits(client, 41, key);
		// 끝이 크기를 넘으면 마지막 바이트까지로 잘린다
		var response = client.getObject(new GetObjectRequest(bucketName, key).withRange(5, 100));
		assertEquals(List.of(5L, 9L), List.of(response.getObjectMetadata().getContentRange()));
		assertEquals(10, response.getObjectMetadata().getInstanceLength());
		assertEquals("56789", getBody(response.getObjectContent()));

		// 시작이 크기와 같으면 416
		// (SDK 가 뒤에서부터의 길이 지정(bytes=-N)을 표현하지 못해 bytes=-100, bytes=-0 은 확인하지 않는다)
		assertS3Error(HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE, MainData.INVALID_RANGE,
				() -> client.getObject(new GetObjectRequest(bucketName, key).withRange(10, 20)));
	}

	@Test
	@Tag("PartNumber")
	public void testGetObjectPartNumber() {
		var key = "testGetObjectPartNumber";
		var client = getClient();
		var bucketName = putDigits(client, 43, key);
		// 멀티파트가 아닌 오브젝트는 1번 파트가 전체다
		var response = client.getObject(new GetObjectRequest(bucketName, key).withPartNumber(1));
		assertEquals(List.of(0L, 9L), List.of(response.getObjectMetadata().getContentRange()));
		assertEquals(10, response.getObjectMetadata().getInstanceLength());
		assertNull(response.getObjectMetadata().getPartCount());
		assertEquals(DIGITS, getBody(response.getObjectContent()));

		var headResponse = client.getObjectMetadata(new GetObjectMetadataRequest(bucketName, key).withPartNumber(1));
		assertEquals(10, headResponse.getContentLength());

		assertS3Error(HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE, MainData.INVALID_PART_NUMBER,
				() -> client.getObject(new GetObjectRequest(bucketName, key).withPartNumber(2)));
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
					() -> client.getObject(new GetObjectRequest(bucketName, key).withPartNumber(partNumber)));
		}
		// Range 와 partNumber 는 함께 쓸 수 없다
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_REQUEST,
				() -> client.getObject(new GetObjectRequest(bucketName, key).withPartNumber(1).withRange(0, 1)));
	}

	@Test
	@Tag("Version")
	public void testGetObjectVersionIdUnversioned() {
		var key = "testGetObjectVersionIdUnversioned";
		var client = getClient();
		var bucketName = putDigits(client, 45, key);
		// 버저닝을 쓰지 않는 버킷의 오브젝트는 null 버전이다
		assertEquals(DIGITS,
				getBody(client.getObject(new GetObjectRequest(bucketName, key, "null")).getObjectContent()));
		assertS3Error(HttpStatus.SC_BAD_REQUEST, MainData.INVALID_ARGUMENT,
				() -> client.getObject(new GetObjectRequest(bucketName, key, "invalid")));
		assertS3Error(HttpStatus.SC_BAD_REQUEST, null,
				() -> client.getObjectMetadata(new GetObjectMetadataRequest(bucketName, key, "invalid")));
	}

	@Test
	@Tag("Version")
	public void testGetObjectNullVersionNotExist() {
		var key = "testGetObjectNullVersionNotExist";
		var client = getClient();
		var bucketName = createBucket(client, 46);
		checkConfigureVersioningRetry(bucketName, BucketVersioningConfiguration.ENABLED);
		client.putObject(bucketName, key, "content");
		// 버저닝을 켠 뒤에 만든 오브젝트에는 null 버전이 없다
		assertS3Error(HttpStatus.SC_NOT_FOUND, MainData.NO_SUCH_VERSION,
				() -> client.getObject(new GetObjectRequest(bucketName, key, "null")));
		assertS3Error(HttpStatus.SC_NOT_FOUND, null,
				() -> client.getObjectMetadata(new GetObjectMetadataRequest(bucketName, key, "null")));
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
				() -> publicClient.getObject(new GetObjectRequest(bucketName, key)
						.withResponseHeaders(new ResponseHeaderOverrides().withContentType("text/html"))));
	}

	@Test
	@Tag("Header")
	public void testGetObjectDefaultHeaders() {
		var key = "testGetObjectDefaultHeaders";
		var client = getClient();
		var bucketName = putDigits(client, 49, key);
		// (SDK 가 Content-Type 을 항상 붙여 올리므로 기본값 binary/octet-stream 은 확인하지 않는다)
		var getResponse = client.getObject(bucketName, key);
		for (var response : List.of(getResponse.getObjectMetadata(), client.getObjectMetadata(bucketName, key))) {
			assertEquals("bytes", response.getRawMetadataValue("Accept-Ranges"));
			assertEquals(10, response.getContentLength());
			assertEquals("781e5e245d69b566979b86e28d23f2c7", response.getETag());
			assertTrue(response.getLastModified() != null);
		}
		getBody(getResponse.getObjectContent());
	}

	@Test
	@Tag("IfMatch")
	public void testGetObjectIfMatchWildcard() {
		var key = "testGetObjectIfMatchWildcard";
		var client = getClient();
		var bucketName = putDigits(client, 50, key);
		// If-Match: * 는 오브젝트가 있으면 통과, If-None-Match: * 는 304(SDK 는 null 을 돌려준다)
		assertEquals(DIGITS, getBody(client
				.getObject(new GetObjectRequest(bucketName, key).withMatchingETagConstraint("*")).getObjectContent()));
		assertNull(client.getObject(new GetObjectRequest(bucketName, key).withNonmatchingETagConstraint("*")));
	}
}
