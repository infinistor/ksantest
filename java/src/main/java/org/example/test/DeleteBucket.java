package org.example.test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;

import org.example.s3tests.MainData;
import org.junit.jupiter.api.DisplayName;
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
	@DisplayName("test_bucket_delete_notexist")
	@Tag("ERROR")
	@Tag("KSAN")
	//@Tag("존재하지 않는 버킷을 삭제하려 했을 경우 실패 확인")
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
	@DisplayName("test_bucket_delete_nonempty")
	@Tag("ERROR")
	@Tag("KSAN")
	//@Tag("내용이 비어있지 않은 버킷을 삭제하려 했을 경우 실패 확인")
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
	@DisplayName("test_bucket_create_delete")
	@Tag("ERROR")
	@Tag("KSAN")
	//@Tag("이미 삭제된 버킷을 다시 삭제 시도할 경우 실패 확인")
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
