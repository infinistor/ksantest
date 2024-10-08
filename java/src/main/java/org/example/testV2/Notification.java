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

import java.util.List;

import org.junit.Test;
import org.junit.jupiter.api.Tag;

import software.amazon.awssdk.services.s3.model.Event;
import software.amazon.awssdk.services.s3.model.LambdaFunctionConfiguration;
import software.amazon.awssdk.services.s3.model.NotificationConfiguration;

public class Notification extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Notification V2 Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Notification V2 End");
	}

	@Test
	@Tag("Get")
	public void testNotificationGetEmpty() {
		var client = getClient();
		var bucketName = createBucket(client);

		var result = client.getBucketNotificationConfiguration(g -> g.bucket(bucketName));

		assertEquals(0, result.lambdaFunctionConfigurations().size());
		assertEquals(0, result.queueConfigurations().size());
		assertEquals(0, result.topicConfigurations().size());
		assertEquals(0, result.queueConfigurations().size());
	}

	@Test
	@Tag("Put")
	public void testNotificationPut() {
		var client = getClient();
		var bucketName = createBucket(client);
		var roleId = "my-lambda";
		var mainUserId = config.mainUser.id;
		var functionARN = "aws:lambda::" + mainUserId + ":function:my-function";
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
		var client = getClient();
		var bucketName = createBucket(client);
		var roleId = "my-lambda";
		var mainUserId = config.mainUser.id;
		var functionARN = "aws:lambda::" + mainUserId + ":function:my-function";
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
		var client = getClient();
		var bucketName = createBucket(client);
		var roleId = "my-lambda";
		var mainUserId = config.mainUser.id;
		var functionARN = "aws:lambda::" + mainUserId + ":function:my-function";
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
}
