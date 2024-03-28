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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ResponseHeaderOverrides;

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
	public void test_object_raw_get() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(CannedAccessControlList.PublicRead, CannedAccessControlList.PublicRead,
				key);

		var unauthenticatedClient = getPublicClient();
		unauthenticatedClient.getObject(bucketName, key);
	}

	@Test
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 삭제된 버킷의 삭제된
	// 오브젝트에 접근할때 에러 확인
	public void test_object_raw_get_bucket_gone() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(CannedAccessControlList.PublicRead, CannedAccessControlList.PublicRead,
				key);
		var client = getClient();

		client.deleteObject(bucketName, key);
		client.deleteBucket(bucketName);

		var unauthenticatedClient = getPublicClient();
		var e = assertThrows(AmazonServiceException.class, () -> unauthenticatedClient.getObject(bucketName, key));

		assertEquals(404, e.getStatusCode());
		assertEquals(MainData.NoSuchBucket, e.getErrorCode());
		DeleteBucketList(bucketName);
	}

	@Test
	@Tag("Delete")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 삭제된 버킷의 삭제된
	// 오브젝트를 삭제할때 에러 확인
	public void test_object_delete_key_bucket_gone() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(CannedAccessControlList.PublicRead, CannedAccessControlList.PublicRead,
				key);
		var client = getClient();

		client.deleteObject(bucketName, key);
		client.deleteBucket(bucketName);

		var unauthenticatedClient = getPublicClient();
		var e = assertThrows(AmazonServiceException.class, () -> unauthenticatedClient.deleteObject(bucketName, key));

		assertEquals(404, e.getStatusCode());
		assertEquals(MainData.NoSuchBucket, e.getErrorCode());
		DeleteBucketList(bucketName);
	}

	@Test
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 권한없는 사용자가 삭제된 오브젝트에 접근할때
	// 에러 확인
	public void test_object_raw_get_object_gone() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(CannedAccessControlList.PublicRead, CannedAccessControlList.PublicRead,
				key);
		var client = getClient();

		client.deleteObject(bucketName, key);

		var unauthenticatedClient = getPublicClient();
		var e = assertThrows(AmazonServiceException.class, () -> unauthenticatedClient.getObject(bucketName, key));

		assertEquals(404, e.getStatusCode());
		assertEquals(MainData.NoSuchKey, e.getErrorCode());
	}

	@Test
	@Tag("Get")
	// [Bucket_ACL = private, Object_ACL = public-read] 권한없는 사용자가 개인버킷의 공용 오브젝트에 접근
	// 가능한지 확인
	public void test_object_raw_get_bucket_acl() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(CannedAccessControlList.Private, CannedAccessControlList.PublicRead, key);

		var unauthenticatedClient = getPublicClient();
		unauthenticatedClient.getObject(bucketName, key);
	}

	@Test
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = private] 권한없는 사용자가 공용버킷의 개인 오브젝트에
	// 접근할때 에러확인
	public void test_object_raw_get_object_acl() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(CannedAccessControlList.PublicRead, CannedAccessControlList.Private, key);

		var unauthenticatedClient = getPublicClient();
		var e = assertThrows(AmazonServiceException.class, () -> unauthenticatedClient.getObject(bucketName, key));

		assertEquals(403, e.getStatusCode());
		assertEquals(MainData.AccessDenied, e.getErrorCode());
	}

	@Test
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 로그인한 사용자가 공용 버킷의 공용
	// 오브젝트에 접근 가능한지 확인
	public void test_object_raw_authenticated() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(CannedAccessControlList.PublicRead, CannedAccessControlList.PublicRead,
				key);

		var client = getClient();
		client.getObject(bucketName, key);
	}

	@Test
	@Tag("Header")
	// [Bucket_ACL = private, Object_ACL = private] 로그인한 사용자가 GetObject의 반환헤더값을 설정하고
	// 개인 오브젝트를 가져올때 반환헤더값이 적용되었는지 확인
	public void test_object_raw_response_headers() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(CannedAccessControlList.Private, CannedAccessControlList.Private, key);
		var client = getClient();

		var Date = new Date();
		SimpleDateFormat rfc822format = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss 'GMT'", Locale.ENGLISH);
		// rfc822format.setTimeZone(TimeZone.getTimeZone("UTC"));
		String Str_Date = rfc822format.format(Date);

		var Response = client.getObject(new GetObjectRequest(bucketName, key).withResponseHeaders(
				new ResponseHeaderOverrides()
						.withCacheControl("no-cache")
						.withContentDisposition("bla")
						.withContentEncoding("aaa")
						.withContentLanguage("esperanto")
						.withContentType("foo/bar")
						.withExpires(Str_Date)));

		assertEquals("no-cache", Response.getObjectMetadata().getCacheControl());
		assertEquals("bla", Response.getObjectMetadata().getContentDisposition());
		assertEquals("aaa", Response.getObjectMetadata().getContentEncoding());
		assertEquals("esperanto", Response.getObjectMetadata().getContentLanguage());
		assertEquals("foo/bar", Response.getObjectMetadata().getContentType());
	}

	@Test
	@Tag("Get")
	// [Bucket_ACL = private, Object_ACL = public-read] 로그인한 사용자가 개인버킷의 공용 오브젝트에 접근
	// 가능한지 확인
	public void test_object_raw_authenticated_bucket_acl() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(CannedAccessControlList.Private, CannedAccessControlList.PublicRead, key);

		var client = getAltClient();
		client.getObject(bucketName, key);
	}

	@Test
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = private] 로그인한 사용자가 공용버킷의 개인 오브젝트에 접근 가능한지 확인
	public void test_object_raw_authenticated_object_acl() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(CannedAccessControlList.PublicRead, CannedAccessControlList.AuthenticatedRead, key);

		var client = getAltClient();
		client.getObject(bucketName, key);
	}

	@Test
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 로그인한 사용자가 삭제된 버킷의 삭제된
	// 오브젝트에 접근할때 에러 확인
	public void test_object_raw_authenticated_bucket_gone() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(CannedAccessControlList.PublicRead, CannedAccessControlList.PublicRead,
				key);
		var client = getClient();

		client.deleteObject(bucketName, key);
		client.deleteBucket(bucketName);

		var e = assertThrows(AmazonServiceException.class, () -> client.getObject(bucketName, key));

		assertEquals(404, e.getStatusCode());
		assertEquals(MainData.NoSuchBucket, e.getErrorCode());
		DeleteBucketList(bucketName);
	}

	@Test
	@Tag("Get")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 로그인한 사용자가 삭제된 오브젝트에 접근할때
	// 에러 확인
	public void test_object_raw_authenticated_object_gone() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(CannedAccessControlList.PublicRead, CannedAccessControlList.PublicRead,
				key);
		var client = getClient();

		client.deleteObject(bucketName, key);

		var e = assertThrows(AmazonServiceException.class, () -> client.getObject(bucketName, key));

		assertEquals(404, e.getStatusCode());
		assertEquals(MainData.NoSuchKey, e.getErrorCode());
	}

	@Test
	@Tag("Post")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 로그인이 만료되지 않은 사용자가 공용 버킷의
	// 공용 오브젝트에 URL 형식으로 접근 가능한지 확인
	public void test_object_raw_get_x_amz_expires_not_expired() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(CannedAccessControlList.PublicRead, CannedAccessControlList.PublicRead,
				key);
		var client = getClient();

		var Address = client.generatePresignedUrl(bucketName, key, getTimeToAddSeconds(100000), HttpMethod.GET);
		var Response = getObject(Address);

		assertEquals(200, Response.getStatusLine().getStatusCode());
	}

	@Test
	@Tag("Post")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 로그인이 만료된 사용자가 공용 버킷의 공용
	// 오브젝트에 URL 형식으로 접근 실패 확인
	public void test_object_raw_get_x_amz_expires_out_range_zero() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(CannedAccessControlList.PublicRead, CannedAccessControlList.PublicRead,
				key);
		var client = getClient();

		var Address = client.generatePresignedUrl(bucketName, key, getTimeToAddSeconds(-1), HttpMethod.GET);
		var Response = getObject(Address);
		assertEquals(403, Response.getStatusLine().getStatusCode());
	}

	@Test
	@Tag("Post")
	// [Bucket_ACL = public-read, Object_ACL = public-read] 로그인 유효주기가 만료된 사용자가 공용
	// 버킷의 공용 오브젝트에 URL 형식으로 접근 실패 확인
	public void test_object_raw_get_x_amz_expires_out_positive_range() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(CannedAccessControlList.PublicRead, CannedAccessControlList.PublicRead, key);
		var client = getClient();

		var address = client.generatePresignedUrl(bucketName, key, getTimeToAddSeconds(-1), HttpMethod.GET);

		var response = getObject(address);
		assertEquals(403, response.getStatusLine().getStatusCode());
	}

	@Test
	@Tag("Update")
	// [Bucket_ACL = Default, Object_ACL = Default] 로그인한 사용자가 버켓을 만들고 업로드한 오브젝트를
	// 권한없는 사용자가 업데이트하려고 할때 실패 확인
	public void test_object_anon_put() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		client.putObject(bucketName, key, "");

		var unauthenticatedClient = getPublicClient();

		var e = assertThrows(AmazonServiceException.class,
				() -> unauthenticatedClient.putObject(bucketName, key, "bar"));
		assertEquals(403, e.getStatusCode());
		assertEquals(MainData.AccessDenied, e.getErrorCode());
	}

	@Test
	@Tag("Update")
	// [Bucket_ACL = public-read-write] 로그인한 사용자가 공용버켓(w/r)을 만들고 업로드한 오브젝트를 권한없는
	// 사용자가 업데이트했을때 올바르게 적용 되는지 확인
	public void test_object_anon_put_write_access() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";

		client.putObject(bucketName, key, "");

		var unauthenticatedClient = getPublicClient();

		var e = assertThrows(AmazonServiceException.class,
				() -> unauthenticatedClient.putObject(bucketName, key, "bar"));
		assertEquals(403, e.getStatusCode());
		assertEquals(MainData.AccessDenied, e.getErrorCode());
	}

	@Test
	@Tag("Default")
	// [Bucket_ACL = Default, Object_ACL = Default] 로그인한 사용자가 버켓을 만들고 업로드
	public void test_object_put_authenticated() {
		var bucketName = getNewBucket();
		var client = getClient();

		client.putObject(bucketName, "foo", "foo");
	}

	@Test
	@Tag("Default")
	// [Bucket_ACL = Default, Object_ACL = Default] Post방식으로 만료된 로그인 정보를 설정하여 오브젝트
	// 업데이트 실패 확인
	public void test_object_raw_put_authenticated_expired() {
		var bucketName = getNewBucket();
		var client = getClient();
		var key = "foo";
		client.putObject(bucketName, key, "");

		var Address = client.generatePresignedUrl(bucketName, key, getTimeToAddSeconds(-1), HttpMethod.PUT);

		var Response = putObject(Address, null);
		assertEquals(403, Response.getStatusLine().getStatusCode());
	}

	@Test
	@Tag("Get")
	// [Bucket_ACL = private, Object_ACL = public-read] 모든 사용자가 개인버킷의 공용 오브젝트에 접근
	// 가능한지 확인
	public void test_acl_private_bucket_public_read_object() {
		var key = "foo";
		var bucketName = setupBucketObjectACL(CannedAccessControlList.Private, CannedAccessControlList.PublicRead, key);

		var client = getClient();
		ACLTest(bucketName, key, client, true);

		var AltClient = getAltClient();
		ACLTest(bucketName, key, AltClient, true);

		var unauthenticatedClient = getPublicClient();
		ACLTest(bucketName, key, unauthenticatedClient, true);
	}
}
