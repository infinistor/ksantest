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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.util.EnumSet;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.Test;
import org.junit.jupiter.api.Tag;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.BucketNotificationConfiguration;
import com.amazonaws.services.s3.model.EventBridgeConfiguration;
import com.amazonaws.services.s3.model.LambdaConfiguration;
import com.amazonaws.services.s3.model.S3Event;
import com.amazonaws.services.s3.model.SetBucketNotificationConfigurationRequest;

public class Notification extends TestBase {
	@Test
	@Tag("Get")
	public void testNotificationGetEmpty() {
		var client = getClient();
		var bucketName = createBucket(client, 1);

		var result = client.getBucketNotificationConfiguration(bucketName);

		assert result.getConfigurations().size() == 0;
	}

	@Test
	@Tag("Put")
	public void testNotificationPut() {
		// AWS는 Lambda ARN 유효성을 검증하므로 실제 리소스 없이는 테스트 불가
		assumeFalse(config.isAWS());
		var client = getClient();
		var bucketName = createBucket(client, 2);
		var functionARN = lambdaFunctionArn();

		var s3Events = EnumSet.noneOf(S3Event.class);
		s3Events.add(S3Event.ObjectCreated);
		s3Events.add(S3Event.ObjectRemoved);
		var notification = new BucketNotificationConfiguration();
		var lambdaFunction = new LambdaConfiguration(functionARN, s3Events);
		notification.addConfiguration("my-lambda", lambdaFunction);

		var request = new SetBucketNotificationConfigurationRequest(bucketName, notification);

		client.setBucketNotificationConfiguration(request);
	}

	@Test
	@Tag("Get")
	public void testNotificationGet() {
		// AWS는 Lambda ARN 유효성을 검증하므로 실제 리소스 없이는 테스트 불가
		assumeFalse(config.isAWS());
		var client = getClient();
		var bucketName = createBucket(client, 3);
		var functionARN = lambdaFunctionArn();

		var s3Events = EnumSet.noneOf(S3Event.class);
		s3Events.add(S3Event.ObjectCreated);
		s3Events.add(S3Event.ObjectRemoved);
		var notification = new BucketNotificationConfiguration();
		var lambdaFunction = new LambdaConfiguration(functionARN, s3Events);
		notification.addConfiguration("my-lambda", lambdaFunction);

		var request = new SetBucketNotificationConfigurationRequest(bucketName, notification);

		client.setBucketNotificationConfiguration(request);

		var result = client.getBucketNotificationConfiguration(bucketName);
		var resultLambda = result.getConfigurationByName("my-lambda");

		s3eventCompare(s3Events.toArray(), resultLambda.getEvents().toArray());
	}

	@Test
	@Tag("Delete")
	public void testNotificationDelete() {
		// AWS는 Lambda ARN 유효성을 검증하므로 실제 리소스 없이는 테스트 불가
		assumeFalse(config.isAWS());
		var client = getClient();
		var bucketName = createBucket(client, 4);
		var functionARN = lambdaFunctionArn();

		var s3Events = EnumSet.noneOf(S3Event.class);
		s3Events.add(S3Event.ObjectCreated);
		s3Events.add(S3Event.ObjectRemoved);
		var notification = new BucketNotificationConfiguration();
		var lambdaFunction = new LambdaConfiguration(functionARN, s3Events);
		notification.addConfiguration("my-lambda", lambdaFunction);

		var request = new SetBucketNotificationConfigurationRequest(bucketName, notification);

		client.setBucketNotificationConfiguration(request);

		var result = client.getBucketNotificationConfiguration(bucketName);
		var resultLambda = result.getConfigurationByName("my-lambda");
		s3eventCompare(s3Events.toArray(), resultLambda.getEvents().toArray());

		var deleteNotification = new BucketNotificationConfiguration();

		var deleteRequest = new SetBucketNotificationConfigurationRequest(bucketName, deleteNotification);

		client.setBucketNotificationConfiguration(deleteRequest);

		var deleteResult = client.getBucketNotificationConfiguration(bucketName);
		var deleteResultLambda = deleteResult.getConfigurationByName("my-lambda");

		assert deleteResultLambda == null;
	}

	@Test
	@Tag("Error")
	public void testNotificationPutInvalidArn() {
		var client = getClient();
		var bucketName = createBucket(client, 5);
		// arn: 접두어와 리전이 없는 잘못된 형식
		var functionARN = "aws:lambda::" + config.mainUser.id + ":function:my-function";

		var s3Events = EnumSet.noneOf(S3Event.class);
		s3Events.add(S3Event.ObjectCreated);
		var notification = new BucketNotificationConfiguration();
		notification.addConfiguration("my-lambda", new LambdaConfiguration(functionARN, s3Events));

		var request = new SetBucketNotificationConfigurationRequest(bucketName, notification);

		var e = assertThrows(AmazonServiceException.class, () -> client.setBucketNotificationConfiguration(request));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.getErrorCode());
	}

	@Test
	@Tag("EventBridge")
	public void testNotificationEventBridge() {
		var client = getClient();
		var bucketName = createBucket(client, 6);

		// EventBridge 는 대상 리소스가 없어 AWS 에서도 그대로 설정된다
		var notification = new BucketNotificationConfiguration();
		notification.setEventBridgeConfiguration(new EventBridgeConfiguration());
		client.setBucketNotificationConfiguration(
				new SetBucketNotificationConfigurationRequest(bucketName, notification));

		var result = client.getBucketNotificationConfiguration(bucketName);
		assertNotNull(result.getEventBridgeConfiguration());
		assertEquals(0, result.getConfigurations().size());

		client.setBucketNotificationConfiguration(
				new SetBucketNotificationConfigurationRequest(bucketName, new BucketNotificationConfiguration()));

		var deleteResult = client.getBucketNotificationConfiguration(bucketName);
		assertNull(deleteResult.getEventBridgeConfiguration());
	}

	/**
	 * Lambda 함수 ARN을 정식 형식(arn:aws:lambda:리전:계정:function:이름)으로 만든다.
	 * AWS는 형식이 틀린 ARN을 400 InvalidArgument 로 거부한다.
	 */
	private String lambdaFunctionArn() {
		var region = (config.regionName == null || config.regionName.isBlank()) ? "us-east-1" : config.regionName;
		return "arn:aws:lambda:" + region + ":" + config.mainUser.getExpectedOwnerId() + ":function:my-function";
	}
}
