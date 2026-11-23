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
package org.example.testV2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.util.List;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.Test;
import org.junit.jupiter.api.Tag;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.model.Event;
import software.amazon.awssdk.services.s3.model.EventBridgeConfiguration;
import software.amazon.awssdk.services.s3.model.LambdaFunctionConfiguration;
import software.amazon.awssdk.services.s3.model.NotificationConfiguration;

public class Notification extends TestBase {
	@Test
	@Tag("Get")
	public void testNotificationGetEmpty() {
		var client = getClient();
		var bucketName = createBucket(client, 1);

		var result = client.getBucketNotificationConfiguration(g -> g.bucket(bucketName));

		assertEquals(0, result.lambdaFunctionConfigurations().size());
		assertEquals(0, result.queueConfigurations().size());
		assertEquals(0, result.topicConfigurations().size());
		assertEquals(0, result.queueConfigurations().size());
	}

	@Test
	@Tag("Put")
	public void testNotificationPut() {
		// AWS는 Lambda ARN 유효성을 검증하므로 실제 리소스 없이는 테스트 불가
		assumeFalse(config.isAWS());
		var client = getClient();
		var bucketName = createBucket(client, 2);
		var roleId = "my-lambda";
		var functionARN = lambdaFunctionArn();
		var s3Events = List.of(Event.S3_OBJECT_CREATED, Event.S3_OBJECT_REMOVED);

		var notification = NotificationConfiguration.builder();
		notification.lambdaFunctionConfigurations(
				LambdaFunctionConfiguration.builder().id(roleId).lambdaFunctionArn(functionARN).events(s3Events)
						.build());

		client.putBucketNotificationConfiguration(
				p -> p.bucket(bucketName).notificationConfiguration(notification.build()));
	}

	@Test
	@Tag("Get")
	public void testNotificationGet() {
		// AWS는 Lambda ARN 유효성을 검증하므로 실제 리소스 없이는 테스트 불가
		assumeFalse(config.isAWS());
		var client = getClient();
		var bucketName = createBucket(client, 3);
		var roleId = "my-lambda";
		var functionARN = lambdaFunctionArn();
		var s3Events = List.of(Event.S3_OBJECT_CREATED, Event.S3_OBJECT_REMOVED);

		var notification = NotificationConfiguration.builder();
		notification.lambdaFunctionConfigurations(
				LambdaFunctionConfiguration.builder().id(roleId).lambdaFunctionArn(functionARN).events(s3Events)
						.build());

		client.putBucketNotificationConfiguration(
				p -> p.bucket(bucketName).notificationConfiguration(notification.build()));

		var result = client.getBucketNotificationConfiguration(g -> g.bucket(bucketName));
		var resultLambda = result.lambdaFunctionConfigurations().get(0);

		s3eventCompare(s3Events, resultLambda.events());
	}

	@Test
	@Tag("Delete")
	public void testNotificationDelete() {
		// AWS는 Lambda ARN 유효성을 검증하므로 실제 리소스 없이는 테스트 불가
		assumeFalse(config.isAWS());
		var client = getClient();
		var bucketName = createBucket(client, 4);
		var roleId = "my-lambda";
		var functionARN = lambdaFunctionArn();
		var s3Events = List.of(Event.S3_OBJECT_CREATED, Event.S3_OBJECT_REMOVED);

		var notification = NotificationConfiguration.builder();
		notification.lambdaFunctionConfigurations(
				LambdaFunctionConfiguration.builder().id(roleId).lambdaFunctionArn(functionARN).events(s3Events)
						.build());

		client.putBucketNotificationConfiguration(
				p -> p.bucket(bucketName).notificationConfiguration(notification.build()));

		var result = client.getBucketNotificationConfiguration(g -> g.bucket(bucketName));
		var resultLambda = result.lambdaFunctionConfigurations().get(0);
		s3eventCompare(s3Events, resultLambda.events());

		client.putBucketNotificationConfiguration(
				p -> p.bucket(bucketName).notificationConfiguration(NotificationConfiguration.builder().build()));

		var deleteResult = client.getBucketNotificationConfiguration(g -> g.bucket(bucketName));
		
		assertEquals(0, deleteResult.lambdaFunctionConfigurations().size());
	}

	@Test
	@Tag("Error")
	public void testNotificationPutInvalidArn() {
		var client = getClient();
		var bucketName = createBucket(client, 5);
		// arn: 접두어와 리전이 없는 잘못된 형식
		var functionARN = "aws:lambda::" + config.mainUser.id + ":function:my-function";

		var notification = NotificationConfiguration.builder()
				.lambdaFunctionConfigurations(LambdaFunctionConfiguration.builder().id("my-lambda")
						.lambdaFunctionArn(functionARN).events(List.of(Event.S3_OBJECT_CREATED)).build())
				.build();

		var e = assertThrows(AwsServiceException.class, () -> client.putBucketNotificationConfiguration(
				p -> p.bucket(bucketName).notificationConfiguration(notification)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("EventBridge")
	public void testNotificationEventBridge() {
		var client = getClient();
		var bucketName = createBucket(client, 6);

		// EventBridge 는 대상 리소스가 없어 AWS 에서도 그대로 설정된다
		client.putBucketNotificationConfiguration(p -> p.bucket(bucketName)
				.notificationConfiguration(n -> n.eventBridgeConfiguration(EventBridgeConfiguration.builder().build())));

		var result = client.getBucketNotificationConfiguration(g -> g.bucket(bucketName));
		assertNotNull(result.eventBridgeConfiguration());
		assertEquals(0, result.lambdaFunctionConfigurations().size());

		client.putBucketNotificationConfiguration(
				p -> p.bucket(bucketName).notificationConfiguration(NotificationConfiguration.builder().build()));

		var deleteResult = client.getBucketNotificationConfiguration(g -> g.bucket(bucketName));
		assertNull(deleteResult.eventBridgeConfiguration());
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
