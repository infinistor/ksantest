package org.example.test;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.services.s3.model.BucketAccelerateConfiguration;
import com.amazonaws.services.s3.model.BucketAccelerateStatus;
import com.amazonaws.services.s3.model.SetBucketAccelerateConfigurationRequest;

public class Accelerate extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll() {
		System.out.println("Accelerate Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll() {
		System.out.println("Accelerate End");
	}

	@Test
	@Tag("Put")
	// 버킷 가속 설정이 가능한지 확인
	public void test_put_bucket_accelerate() {
		var bucketName = getNewBucket();
		var client = getClient();

		client.setBucketAccelerateConfiguration(new SetBucketAccelerateConfigurationRequest(bucketName, null)
				.withBucketName(bucketName)
				.withAccelerateConfiguration(new BucketAccelerateConfiguration(BucketAccelerateStatus.Enabled)));
	}

	@Test
	@Tag("Get")
	// 버킷 가속 설정이 올바르게 적용되는지 확인
	public void test_get_bucket_accelerate() {
		var bucketName = getNewBucket();
		var client = getClient();

		client.setBucketAccelerateConfiguration(new SetBucketAccelerateConfigurationRequest(bucketName, null)
				.withBucketName(bucketName)
				.withAccelerateConfiguration(new BucketAccelerateConfiguration(BucketAccelerateStatus.Enabled)));

		var response = client.getBucketAccelerateConfiguration(bucketName);
		assertEquals("Enabled", response.getStatus());
	}

	@Test
	@Tag("Change")
	// 버킷 가속 설정이 변경되는지 확인
	public void test_delete_bucket_accelerate() {
		var bucketName = getNewBucket();
		var client = getClient();

		client.setBucketAccelerateConfiguration(new SetBucketAccelerateConfigurationRequest(bucketName, null)
				.withBucketName(bucketName)
				.withAccelerateConfiguration(new BucketAccelerateConfiguration(BucketAccelerateStatus.Enabled)));

		var response = client.getBucketAccelerateConfiguration(bucketName);
		assertEquals("Enabled", response.getStatus());

		client.setBucketAccelerateConfiguration(new SetBucketAccelerateConfigurationRequest(bucketName, null)
				.withBucketName(bucketName)
				.withAccelerateConfiguration(new BucketAccelerateConfiguration(BucketAccelerateStatus.Suspended)));

		response = client.getBucketAccelerateConfiguration(bucketName);
		assertEquals("Suspended", response.getStatus());
	}
}
