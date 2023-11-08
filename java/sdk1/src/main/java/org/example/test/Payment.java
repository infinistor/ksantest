package org.example.test;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.services.s3.model.RequestPaymentConfiguration;
import com.amazonaws.services.s3.model.SetRequestPaymentConfigurationRequest;
import com.amazonaws.services.s3.model.RequestPaymentConfiguration.Payer;

public class Payment extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll() {
		System.out.println("Payment Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll() {
		System.out.println("Payment End");
	}

	@Test
	@Tag("Put")
	// 버킷 과금 설정이 가능한지 확인
	public void test_put_bucket_request_payment() {
		var bucketName = getNewBucket();
		var client = getClient();

		client.setRequestPaymentConfiguration(new SetRequestPaymentConfigurationRequest(bucketName, new RequestPaymentConfiguration(Payer.Requester)));
	}

	@Test
	@Tag("Get")
	// 버킷 과금 설정 조회 확인
	public void test_get_bucket_request_payment() {
		var bucketName = getNewBucket();
		var client = getClient();

		var result = client.isRequesterPaysEnabled(bucketName);
		assertEquals(false, result);
	}

	@Test
	@Tag("Get")
	// 버킷 과금 설정이 올바르게 적용되는지 확인
	public void test_set_get_bucket_request_payment() {
		var bucketName = getNewBucket();
		var client = getClient();

		client.setRequestPaymentConfiguration(new SetRequestPaymentConfigurationRequest(bucketName, new RequestPaymentConfiguration(Payer.Requester)));
		var result = client.isRequesterPaysEnabled(bucketName);
		assertEquals(true, result);
	}
	
}
