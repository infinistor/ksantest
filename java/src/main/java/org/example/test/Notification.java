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

import java.util.EnumSet;

import org.junit.Test;
import org.junit.jupiter.api.Tag;

import com.amazonaws.services.s3.model.BucketNotificationConfiguration;
import com.amazonaws.services.s3.model.LambdaConfiguration;
import com.amazonaws.services.s3.model.S3Event;
import com.amazonaws.services.s3.model.SetBucketNotificationConfigurationRequest;

public class Notification extends TestBase {
	@org.junit.jupiter.api.BeforeAll
	public static void beforeAll() {
		System.out.println("Notification Start");
	}

	@org.junit.jupiter.api.AfterAll
	public static void afterAll() {
		System.out.println("Notification End");
	}

	@Test
	@Tag("Get")
	public void testNotificationGetEmpty() {
		var client = getClient();
		var bucketName = createBucket(client);

		var result = client.getBucketNotificationConfiguration(bucketName);

		assert result.getConfigurations().size() == 0;
	}

	@Test
	@Tag("Put")
	public void testNotificationPut() {
		var client = getClient();
		var bucketName = createBucket(client);
		var mainUserId = config.mainUser.id;
		var functionARN = "aws:lambda::" + mainUserId + ":function:my-function";

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
		var client = getClient();
		var bucketName = createBucket(client);
		var mainUserId = config.mainUser.id;
		var functionARN = "aws:lambda::" + mainUserId + ":function:my-function";

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
		var client = getClient();
		var bucketName = createBucket(client);
		var mainUserId = config.mainUser.id;
		var functionARN = "aws:lambda::" + mainUserId + ":function:my-function";

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
}
