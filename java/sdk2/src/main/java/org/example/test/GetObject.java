/*
* Copyright (c) 2021 PSPACE, inc. KSAN Development Team ksan@pspace.co.kr
* KSAN is a suite of free software: you can redistribute it and/or modify it under the terms of
* the GNU General Public License as published by the Free Software Foundation, either version
* 3 of the License.  See LICENSE for details
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

import java.util.Calendar;

import org.example.Data.MainData;
import org.example.Utility.Utils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.RestoreObjectRequest;

public class GetObject extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll() {
		System.out.println("GetObject Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll() {
		System.out.println("GetObject End");
	}

	@Test
	@Tag("ERROR")
	// 버킷에 존재하지 않는 오브젝트 다운로드를 할 경우 실패 확인
	public void test_object_read_not_exist() {
		var bucketName = getNewBucket();
		var client = getClient();

		var e = assertThrows(AmazonServiceException.class, () -> client.getObject(bucketName, "bar"));
		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();

		assertEquals(404, statusCode);
		assertEquals(MainData.NoSuchKey, errorCode);
	}

	@Test
	@Tag("Ifmatch")
	// 존재하는 오브젝트 이름과 ETag 값으로 오브젝트를 가져오는지 확인
	public void test_get_object_ifmatch_good() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		var putResponse = client.putObject(bucketName, key, "bar");
		var eTag = putResponse.getETag();

		var getResponse = client.getObject(new GetObjectRequest(bucketName, key).withMatchingETagConstraint(eTag));
		var body = getBody(getResponse.getObjectContent());
		assertEquals("bar", body);
	}

	@Test
	@Tag("Ifmatch")
	// 오브젝트와 일치하지 않는 ETag 값을 설정하여 오브젝트 조회 실패 확인
	public void test_get_object_ifmatch_failed() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		client.putObject(bucketName, key, "bar");
		var response = client.getObject(new GetObjectRequest(bucketName, key).withMatchingETagConstraint("ABCORZ"));
		assertNull(response);
	}

	@Test
	@Tag("Ifnonematch")
	// 오브젝트와 일치하는 ETag 값을 IfsNoneMatch에 설정하여 오브젝트 조회 실패
	public void test_get_object_ifnonematch_good() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		var putResponse = client.putObject(bucketName, key, "bar");
		var eTag = putResponse.getETag();

		var response = client.getObject(new GetObjectRequest(bucketName, key).withNonmatchingETagConstraint(eTag));
		assertNull(response);
	}

	@Test
	@Tag("Ifnonematch")
	// 오브젝트와 일치하지 않는 ETag 값을 IfsNoneMatch에 설정하여 오브젝트 조회 성공
	public void test_get_object_ifnonematch_failed() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		client.putObject(bucketName, key, "bar");

		var getResponse = client
				.getObject(new GetObjectRequest(bucketName, key).withNonmatchingETagConstraint("ABCORZ"));
		var body = getBody(getResponse.getObjectContent());
		assertEquals("bar", body);
	}

	@Test
	@Tag("Ifmodifiedsince")
	// [지정일을 오브젝트 업로드 시간 이전으로 설정] 지정일(ifmodifiedsince)보다 이후에 수정된 오브젝트를 조회 성공
	public void test_get_object_ifmodifiedsince_good() {
		var bucketName = getNewBucket();
		var client = getClient();
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
	@Tag("Ifmodifiedsince")
	// [지정일을 오브젝트 업로드 시간 이후로 설정] 지정일(ifmodifiedsince)보다 이전에 수정된 오브젝트 조회 실패
	public void test_get_object_ifmodifiedsince_failed() {
		var bucketName = getNewBucket();
		var client = getClient();
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
	@Tag("Ifunmodifiedsince")
	// [지정일을 오브젝트 업로드 시간 이전으로 설정] 지정일(ifunmodifiedsince) 이후 수정되지 않은 오브젝트 조회 실패
	public void test_get_object_ifunmodifiedsince_good() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		client.putObject(bucketName, key, "bar");

		var days = Calendar.getInstance();
		days.set(1994, 8, 29, 19, 43, 31);

		var response = client
				.getObject(new GetObjectRequest(bucketName, key).withUnmodifiedSinceConstraint(days.getTime()));
		assertNull(response);
	}

	@Test
	@Tag("Ifunmodifiedsince")
	// [지정일을 오브젝트 업로드 시간 이후으로 설정] 지정일(ifunmodifiedsince) 이후 수정되지 않은 오브젝트 조회 성공
	public void test_get_object_ifunmodifiedsince_failed() {
		var bucketName = getNewBucket();
		var client = getClient();
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
	// 지정한 범위로 오브젝트 다운로드가 가능한지 확인
	public void test_ranged_request_response_code() {
		var key = "obj";
		var content = "testcontent";

		var bucketName = getNewBucket();
		var client = getClient();

		client.putObject(bucketName, key, content);
		var response = client.getObject(new GetObjectRequest(bucketName, key).withRange(4, 7));

		var fetchedContent = getBody(response.getObjectContent());
		assertEquals(content.substring(4, 8), fetchedContent);
		assertEquals("bytes 4-7/11", response.getObjectMetadata().getRawMetadataValue(Headers.CONTENT_RANGE));
	}

	@Test
	@Tag("Range")
	// 지정한 범위로 대용량인 오브젝트 다운로드가 가능한지 확인
	public void test_ranged_big_request_response_code() {
		var key = "testobj";
		var content = Utils.randomTextToLong(8 * MainData.MB);

		var bucketName = getNewBucket();
		var client = getClient();

		client.putObject(bucketName, key, content);
		var response = client.getObject(new GetObjectRequest(bucketName, key).withRange(3145728, 5242880));

		var fetchedContent = getBody(response.getObjectContent());
		assertTrue(content.substring(3145728, 5242881).equals(fetchedContent), MainData.NOT_MATCHED);
		assertEquals("bytes 3145728-5242880/8388608",
				response.getObjectMetadata().getRawMetadataValue(Headers.CONTENT_RANGE));
	}

	@Test
	@Tag("Range")
	// 특정지점부터 끝까지 오브젝트 다운로드 가능한지 확인
	public void test_ranged_request_skip_leading_bytes_response_code() {
		var key = "testobj";
		var content = "testcontent";

		var bucketName = getNewBucket();
		var client = getClient();

		client.putObject(bucketName, key, content);
		var response = client.getObject(new GetObjectRequest(bucketName, key).withRange(4));

		var fetchedContent = getBody(response.getObjectContent());
		assertEquals(content.substring(4), fetchedContent);
		assertEquals("bytes 4-10/11", response.getObjectMetadata().getRawMetadataValue(Headers.CONTENT_RANGE));
	}

	@Test
	@Tag("Range")
	// 끝에서 부터 특정 길이까지 오브젝트 다운로드 가능한지 확인
	public void test_ranged_request_return_trailing_bytes_response_code() {
		var key = "testobj";
		var content = "testcontent";

		var bucketName = getNewBucket();
		var client = getClient();

		client.putObject(bucketName, key, content);
		var response = client.getObject(new GetObjectRequest(bucketName, key).withRange(4));

		var fetchedContent = getBody(response.getObjectContent());
		assertEquals(content.substring(content.length() - 7), fetchedContent);
		assertEquals("bytes 4-10/11", response.getObjectMetadata().getRawMetadataValue(Headers.CONTENT_RANGE));
	}

	@Test
	@Tag("Range")
	// 오브젝트의 크기를 초과한 범위를 설정하여 다운로드 할경우 실패 확인
	public void test_ranged_request_invalid_range() {
		var key = "testobj";
		var content = "testcontent";

		var bucketName = getNewBucket();
		var client = getClient();

		client.putObject(bucketName, key, content);
		var e = assertThrows(AmazonServiceException.class,
				() -> client.getObject(new GetObjectRequest(bucketName, key).withRange(40, 50)));
		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();
		assertEquals(416, statusCode);
		assertEquals(MainData.InvalidRange, errorCode);
	}

	@Test
	@Tag("Range")
	// 비어있는 오브젝트를 범위를 지정하여 다운로드 실패 확인
	public void test_ranged_request_empty_object() {
		var key = "testobj";
		var content = "";

		var bucketName = getNewBucket();
		var client = getClient();

		client.putObject(bucketName, key, content);
		var e = assertThrows(AmazonServiceException.class,
				() -> client.getObject(new GetObjectRequest(bucketName, key).withRange(40, 50)));
		var statusCode = e.getStatusCode();
		var errorCode = e.getErrorCode();
		assertEquals(416, statusCode);
		assertEquals(MainData.InvalidRange, errorCode);
	}

	@Test
	@Tag("Get")
	// 같은 오브젝트를 여러번 반복하여 다운로드 성공 확인
	public void test_get_object_many() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";
		var Data = Utils.randomTextToLong(15 * MainData.MB);

		client.putObject(bucketName, key, Data);
		checkContent(bucketName, key, Data, 50);
	}

	@Test
	@Tag("Get")
	// 같은 오브젝트를 여러번 반복하여 Range 다운로드 성공 확인
	public void test_range_object_many() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";
		var FileSize = 1024 * 1024 * 15;
		var Data = Utils.randomTextToLong(FileSize);

		client.putObject(bucketName, key, Data);
		checkContentUsingRandomRange(bucketName, key, Data, 50);
	}

	@Test
	@Tag("Restore")
	//오브젝트 복구 명령이 성공하는지 확인
	public void test_restore_object() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		client.putObject(bucketName, key, key);
		
		var Request = new RestoreObjectRequest(bucketName, key);
		client.restoreObjectV2(Request);
	}
}
