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

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.services.s3.model.RequestPaymentConfiguration;
import com.amazonaws.services.s3.model.SetRequestPaymentConfigurationRequest;
import com.amazonaws.services.s3.model.RequestPaymentConfiguration.Payer;

public class Payment extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Payment Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Payment End");
	}

	@Test
	@Tag("Put")
	// 버킷 과금 설정이 가능한지 확인
	public void testPutBucketRequestPayment() {
		var bucketName = getNewBucket();
		var client = getClient();

		client.setRequestPaymentConfiguration(new SetRequestPaymentConfigurationRequest(bucketName, new RequestPaymentConfiguration(Payer.Requester)));
	}

	@Test
	@Tag("Get")
	// 버킷 과금 설정 조회 확인
	public void testGetBucketRequestPayment() {
		var bucketName = getNewBucket();
		var client = getClient();

		var result = client.isRequesterPaysEnabled(bucketName);
		assertEquals(false, result);
	}

	@Test
	@Tag("Get")
	// 버킷 과금 설정이 올바르게 적용되는지 확인
	public void testSetGetBucketRequestPayment() {
		var bucketName = getNewBucket();
		var client = getClient();

		client.setRequestPaymentConfiguration(new SetRequestPaymentConfigurationRequest(bucketName, new RequestPaymentConfiguration(Payer.Requester)));
		var result = client.isRequesterPaysEnabled(bucketName);
		assertEquals(true, result);
	}
	
}
