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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.example.Data.MainData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.BucketLoggingConfiguration;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.model.ServerSideEncryptionByDefault;
import com.amazonaws.services.s3.model.ServerSideEncryptionConfiguration;
import com.amazonaws.services.s3.model.ServerSideEncryptionRule;
import com.amazonaws.services.s3.model.SetBucketEncryptionRequest;
import com.amazonaws.services.s3.model.SetBucketLoggingConfigurationRequest;

public class Logging extends TestBase
{
	@org.junit.jupiter.api.BeforeAll
	static public void BeforeAll()
	{
		System.out.println("Logging Start");
	}

	@org.junit.jupiter.api.AfterAll
	static public void AfterAll()
	{
		System.out.println("Logging End");
	}

	@Test
	@Tag("Put/Get")
	// 버킷에 로깅 설정 조회 가능한지 확인
	public void test_logging_get()
	{
		var bucketName = getNewBucket();
		var client = getClient();

		var Response = client.getBucketLoggingConfiguration(bucketName);
		assertNull(Response.getLogFilePrefix());
		assertNull(Response.getDestinationBucketName());
	}

	@Test
	@Tag("Put/Get")
	// 버킷에 로깅 설정 가능한지 확인
	public void test_logging_set()
	{
		var SourceBucketName = getNewBucket();
		var TargetBucketName = getNewBucket();
		var client = getClient();

		var Request = new SetBucketLoggingConfigurationRequest(SourceBucketName, new BucketLoggingConfiguration(TargetBucketName, ""));
		client.setBucketLoggingConfiguration(Request);
	}

	@Test
	@Tag("Put/Get")
	// 버킷에 설정한 로깅 정보 조회가 가능한지 확인
	public void test_logging_set_get()
	{
		var SourceBucketName = getNewBucket();
		var TargetBucketName = getNewBucket();
		var client = getClient();

		var Request = new SetBucketLoggingConfigurationRequest(SourceBucketName, new BucketLoggingConfiguration(TargetBucketName, ""));
		client.setBucketLoggingConfiguration(Request);

		var Response = client.getBucketLoggingConfiguration(SourceBucketName);
		assertEquals("", Response.getLogFilePrefix());
		assertEquals(TargetBucketName, Response.getDestinationBucketName());
	}
	
	@Test
	@Tag("Put/Get")
	// 버킷의 로깅에 Prefix가 설정되는지 확인
	public void test_logging_prefix()
	{
		var SourceBucketName = getNewBucket();
		var TargetBucketName = getNewBucket();
		var Prefix = "logs/";
		var client = getClient();

		var Request = new SetBucketLoggingConfigurationRequest(SourceBucketName, new BucketLoggingConfiguration(TargetBucketName, Prefix));
		client.setBucketLoggingConfiguration(Request);

		var Response = client.getBucketLoggingConfiguration(SourceBucketName);
		assertEquals(Prefix, Response.getLogFilePrefix());
		assertEquals(TargetBucketName, Response.getDestinationBucketName());
	}

	@Test
	@Tag("Versioning")
	// 버저닝 설정된 버킷의 로깅이 설정되는지 확인
	public void test_logging_versioning()
	{
		var SourceBucketName = getNewBucket();
		var TargetBucketName = getNewBucket();
		var Prefix = "logs/";
		var client = getClient();
		
		checkConfigureVersioningRetry(SourceBucketName, BucketVersioningConfiguration.ENABLED);

		var Request = new SetBucketLoggingConfigurationRequest(SourceBucketName, new BucketLoggingConfiguration(TargetBucketName, Prefix));
		client.setBucketLoggingConfiguration(Request);

		var Response = client.getBucketLoggingConfiguration(SourceBucketName);
		assertEquals(Prefix, Response.getLogFilePrefix());
		assertEquals(TargetBucketName, Response.getDestinationBucketName());
	}

	@Test
	@Tag("Encryption")
	// SSE-s3설정된 버킷의 로깅이 설정되는지 확인
	public void test_logging_encryption()
	{
		var SourceBucketName = getNewBucket();
		var TargetBucketName = getNewBucket();
		var Prefix = "logs/";
		var client = getClient();

		var SSES3Config = new ServerSideEncryptionConfiguration()
			.withRules(new ServerSideEncryptionRule()
			.withApplyServerSideEncryptionByDefault(new ServerSideEncryptionByDefault()
			.withSSEAlgorithm(SSEAlgorithm.AES256)).withBucketKeyEnabled(false));
		client.setBucketEncryption(new SetBucketEncryptionRequest().withBucketName(SourceBucketName).withServerSideEncryptionConfiguration(SSES3Config));

		var Request = new SetBucketLoggingConfigurationRequest(SourceBucketName, new BucketLoggingConfiguration(TargetBucketName, Prefix));
		client.setBucketLoggingConfiguration(Request);

		var Response = client.getBucketLoggingConfiguration(SourceBucketName);
		assertEquals(Prefix, Response.getLogFilePrefix());
		assertEquals(TargetBucketName, Response.getDestinationBucketName());
	}
	
	@Test
	@Tag("Error")
	// 존재하지 않는 버킷에 로깅 설정 실패 확인
	public void test_logging_bucket_not_found()
	{
		var SourceBucketName = getNewBucketNameOnly();
		var TargetBucketName = getNewBucketNameOnly();
		var Prefix = "logs/";
		var client = getClient();

		var Request = new SetBucketLoggingConfigurationRequest(SourceBucketName, new BucketLoggingConfiguration(TargetBucketName, Prefix));
		var e = assertThrows(AmazonServiceException.class, () -> client.setBucketLoggingConfiguration(Request));

		assertEquals(404, e.getStatusCode());
		assertEquals(MainData.NoSuchBucket, e.getErrorCode());
	}
	
	@Test
	@Tag("Error")
	// 타깃 버킷이 존재하지 않을때 로깅 설정 실패 확인
	public void test_logging_target_bucket_not_found()
	{
		var SourceBucketName = getNewBucket();
		var TargetBucketName = getNewBucketNameOnly();
		var Prefix = "logs/";
		var client = getClient();

		var Request = new SetBucketLoggingConfigurationRequest(SourceBucketName, new BucketLoggingConfiguration(TargetBucketName, Prefix));
		var e = assertThrows(AmazonServiceException.class, () -> client.setBucketLoggingConfiguration(Request));

		assertEquals(400, e.getStatusCode());
		assertEquals(MainData.InvalidTargetBucketForLogging, e.getErrorCode());
	}
	
}
