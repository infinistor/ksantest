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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;

import org.example.s3tests.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;

public class DeleteBucket extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll()
	{
		System.out.println("DeleteBucket Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll()
	{
		System.out.println("DeleteBucket End");
	}

	@Test
	@Tag("ERROR")
	@Tag("KSAN")
	//존재하지 않는 버킷을 삭제하려 했을 경우 실패 확인
	public void test_bucket_delete_notexist() {
		var BucketName = GetNewBucketName();
		var Client = GetClient();

		var e = assertThrows(AmazonServiceException.class, () -> Client.deleteBucket(BucketName));

		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();

		assertEquals(404, StatusCode);
		assertEquals(MainData.NoSuchBucket, ErrorCode);
		DeleteBucketList(BucketName);
	}

	@Test
	@Tag("ERROR")
	@Tag("KSAN")
	//내용이 비어있지 않은 버킷을 삭제하려 했을 경우 실패 확인
	public void test_bucket_delete_nonempty() {
		var BucketName = CreateObjects(new ArrayList<>(Arrays.asList(new String[] { "foo" })));
		var Client = GetClient();

		var e = assertThrows(AmazonServiceException.class, () -> Client.deleteBucket(BucketName));

		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();

		assertEquals(409, StatusCode);
		assertEquals(MainData.BucketNotEmpty, ErrorCode);
	}

	@Test
	@Tag("ERROR")
	@Tag("KSAN")
	//이미 삭제된 버킷을 다시 삭제 시도할 경우 실패 확인
	public void test_bucket_create_delete() {
		var BucketName = GetNewBucket();
		var Client = GetClient();

		Client.deleteBucket(BucketName);

		var e = assertThrows(AmazonServiceException.class, () -> Client.deleteBucket(BucketName));

		var StatusCode = e.getStatusCode();
		var ErrorCode = e.getErrorCode();

		assertEquals(404, StatusCode);
		assertEquals(MainData.NoSuchBucket, ErrorCode);
		DeleteBucketList(BucketName);
	}
}
