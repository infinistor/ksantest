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

import java.util.ArrayList;
import java.util.List;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.Test;
import org.junit.jupiter.api.Tag;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Event;
import software.amazon.awssdk.services.s3.model.EventBridgeConfiguration;
import software.amazon.awssdk.services.s3.model.FilterRule;
import software.amazon.awssdk.services.s3.model.LambdaFunctionConfiguration;
import software.amazon.awssdk.services.s3.model.NotificationConfiguration;
import software.amazon.awssdk.services.s3.model.QueueConfiguration;
import software.amazon.awssdk.services.s3.model.TopicConfiguration;

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
		var client = getClient();
		var bucketName = createBucket(client, 2);
		var roleId = "my-lambda";
		var functionARN = lambdaFunctionArn();
		var s3Events = List.of(Event.S3_OBJECT_CREATED, Event.S3_OBJECT_REMOVED);

		var notification = NotificationConfiguration.builder();
		notification.lambdaFunctionConfigurations(
				LambdaFunctionConfiguration.builder().id(roleId).lambdaFunctionArn(functionARN).events(s3Events)
						.build());

		putNotification(client, bucketName, notification.build());
	}

	@Test
	@Tag("Get")
	public void testNotificationGet() {
		var client = getClient();
		var bucketName = createBucket(client, 3);
		var roleId = "my-lambda";
		var functionARN = lambdaFunctionArn();
		var s3Events = List.of(Event.S3_OBJECT_CREATED, Event.S3_OBJECT_REMOVED);

		var notification = NotificationConfiguration.builder();
		notification.lambdaFunctionConfigurations(
				LambdaFunctionConfiguration.builder().id(roleId).lambdaFunctionArn(functionARN).events(s3Events)
						.build());

		putNotification(client, bucketName, notification.build());

		var result = client.getBucketNotificationConfiguration(g -> g.bucket(bucketName));
		var resultLambda = result.lambdaFunctionConfigurations().get(0);

		s3eventCompare(s3Events, resultLambda.events());
	}

	@Test
	@Tag("Delete")
	public void testNotificationDelete() {
		var client = getClient();
		var bucketName = createBucket(client, 4);
		var roleId = "my-lambda";
		var functionARN = lambdaFunctionArn();
		var s3Events = List.of(Event.S3_OBJECT_CREATED, Event.S3_OBJECT_REMOVED);

		var notification = NotificationConfiguration.builder();
		notification.lambdaFunctionConfigurations(
				LambdaFunctionConfiguration.builder().id(roleId).lambdaFunctionArn(functionARN).events(s3Events)
						.build());

		putNotification(client, bucketName, notification.build());

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

		var e = assertThrows(AwsServiceException.class, () -> putNotification(client, bucketName, notification));
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

	@Test
	@Tag("Queue")
	public void testNotificationQueue() {
		var client = getClient();
		var bucketName = createBucket(client, 7);
		var queueArn = arn("sqs", "my-queue");
		var s3Events = List.of(Event.S3_OBJECT_CREATED, Event.S3_OBJECT_REMOVED);

		putNotification(client, bucketName, NotificationConfiguration.builder()
				.queueConfigurations(
						QueueConfiguration.builder().id("my-queue").queueArn(queueArn).events(s3Events).build())
				.build());

		var result = client.getBucketNotificationConfiguration(g -> g.bucket(bucketName));
		assertEquals(1, result.queueConfigurations().size());
		assertEquals("my-queue", result.queueConfigurations().get(0).id());
		assertEquals(queueArn, result.queueConfigurations().get(0).queueArn());
		s3eventCompare(s3Events, result.queueConfigurations().get(0).events());

		putNotification(client, bucketName, NotificationConfiguration.builder().build());
		var deleteResult = client.getBucketNotificationConfiguration(g -> g.bucket(bucketName));
		assertEquals(0, deleteResult.queueConfigurations().size());
	}

	@Test
	@Tag("Topic")
	public void testNotificationTopic() {
		var client = getClient();
		var bucketName = createBucket(client, 8);
		var topicArn = arn("sns", "my-topic");
		var s3Events = List.of(Event.S3_OBJECT_CREATED, Event.S3_OBJECT_REMOVED);

		putNotification(client, bucketName, NotificationConfiguration.builder()
				.topicConfigurations(
						TopicConfiguration.builder().id("my-topic").topicArn(topicArn).events(s3Events).build())
				.build());

		var result = client.getBucketNotificationConfiguration(g -> g.bucket(bucketName));
		assertEquals(1, result.topicConfigurations().size());
		assertEquals("my-topic", result.topicConfigurations().get(0).id());
		assertEquals(topicArn, result.topicConfigurations().get(0).topicArn());
		s3eventCompare(s3Events, result.topicConfigurations().get(0).events());

		putNotification(client, bucketName, NotificationConfiguration.builder().build());
		var deleteResult = client.getBucketNotificationConfiguration(g -> g.bucket(bucketName));
		assertEquals(0, deleteResult.topicConfigurations().size());
	}

	@Test
	@Tag("Filter")
	public void testNotificationFilter() {
		var client = getClient();
		var bucketName = createBucket(client, 9);
		var rules = List.of(filterRule("prefix", "logs/"), filterRule("suffix", ".txt"));

		putNotification(client, bucketName, lambdaNotification(lambdaConfig("my-lambda", null, rules)));

		var result = client.getBucketNotificationConfiguration(g -> g.bucket(bucketName));
		var resultRules = result.lambdaFunctionConfigurations().get(0).filter().key().filterRules();
		// AWS 는 필터 이름을 Prefix/Suffix 로 정규화해 돌려주므로 대소문자를 무시하고 비교한다
		var actual = new ArrayList<String>();
		for (var rule : resultRules)
			actual.add(rule.nameAsString().toLowerCase() + "=" + rule.value());
		actual.sort(null);
		assertEquals(List.of("prefix=logs/", "suffix=.txt"), actual);
	}

	@Test
	@Tag("Error")
	public void testNotificationDestinationNotExist() {
		var client = getClient();
		var bucketName = createBucket(client, 10);
		var notification = lambdaNotification(lambdaConfig("my-lambda", null, null));

		// 대상 검증을 건너뛰지 않으면 존재하지 않는 대상은 거부된다
		var e = assertThrows(AwsServiceException.class, () -> client.putBucketNotificationConfiguration(
				p -> p.bucket(bucketName).notificationConfiguration(notification)));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testNotificationInvalidEvent() {
		var client = getClient();
		var bucketName = createBucket(client, 11);
		var notification = lambdaNotification(lambdaConfig("my-lambda", List.of("s3:Foo:*"), null));

		var e = assertThrows(AwsServiceException.class, () -> putNotification(client, bucketName, notification));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testNotificationInvalidFilterName() {
		var client = getClient();
		var bucketName = createBucket(client, 12);
		var notification = lambdaNotification(lambdaConfig("my-lambda", null, List.of(filterRule("foo", "a"))));

		var e = assertThrows(AwsServiceException.class, () -> putNotification(client, bucketName, notification));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testNotificationOverlappingEvent() {
		var client = getClient();
		var bucketName = createBucket(client, 13);

		// 필터 없이 같은 이벤트를 두 설정이 공유하면 거부된다
		var overlapping = lambdaNotification(lambdaConfig("c1", null, null), lambdaConfig("c2", null, null));
		var e = assertThrows(AwsServiceException.class, () -> putNotification(client, bucketName, overlapping));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());

		// 접두어가 겹치지 않으면 같은 이벤트라도 허용된다
		putNotification(client, bucketName,
				lambdaNotification(lambdaConfig("c1", null, List.of(filterRule("prefix", "a/"))),
						lambdaConfig("c2", null, List.of(filterRule("prefix", "b/")))));

		var result = client.getBucketNotificationConfiguration(g -> g.bucket(bucketName));
		var ids = new ArrayList<String>();
		for (var lambda : result.lambdaFunctionConfigurations())
			ids.add(lambda.id());
		ids.sort(null);
		assertEquals(List.of("c1", "c2"), ids);
	}

	@Test
	@Tag("Error")
	public void testNotificationDuplicateId() {
		var client = getClient();
		var bucketName = createBucket(client, 14);
		var notification = lambdaNotification(lambdaConfig("c1", null, List.of(filterRule("prefix", "a/"))),
				lambdaConfig("c1", null, List.of(filterRule("prefix", "b/"))));

		var e = assertThrows(AwsServiceException.class, () -> putNotification(client, bucketName, notification));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.statusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Error")
	public void testNotificationBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(15);

		var e = assertThrows(AwsServiceException.class,
				() -> client.getBucketNotificationConfiguration(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class,
				() -> putNotification(client, bucketName, NotificationConfiguration.builder().build()));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.statusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.awsErrorDetails().errorCode());
	}

	@Test
	@Tag("Permission")
	public void testNotificationAltUser() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 16);

		var e = assertThrows(AwsServiceException.class,
				() -> altClient.getBucketNotificationConfiguration(g -> g.bucket(bucketName)));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());

		e = assertThrows(AwsServiceException.class,
				() -> putNotification(altClient, bucketName, NotificationConfiguration.builder().build()));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.statusCode());
		assertEquals(MainData.ACCESS_DENIED, e.awsErrorDetails().errorCode());
	}

	/**
	 * Lambda 함수 ARN을 정식 형식(arn:aws:lambda:리전:계정:function:이름)으로 만든다.
	 * AWS는 형식이 틀린 ARN을 400 InvalidArgument 로 거부한다.
	 */
	private String lambdaFunctionArn() {
		var region = (config.regionName == null || config.regionName.isBlank()) ? "us-east-1" : config.regionName;
		return "arn:aws:lambda:" + region + ":" + config.mainUser.getExpectedOwnerId() + ":function:my-function";
	}

	/** 대상 리소스 검증을 건너뛰고 알림 설정을 저장한다 (x-amz-skip-destination-validation). */
	private void putNotification(S3Client client, String bucketName, NotificationConfiguration notification) {
		client.putBucketNotificationConfiguration(p -> p.bucket(bucketName).notificationConfiguration(notification)
				.skipDestinationValidation(true));
	}

	private String arn(String service, String resource) {
		var region = (config.regionName == null || config.regionName.isBlank()) ? "us-east-1" : config.regionName;
		return "arn:aws:" + service + ":" + region + ":" + config.mainUser.getExpectedOwnerId() + ":" + resource;
	}

	private static FilterRule filterRule(String name, String value) {
		return FilterRule.builder().name(name).value(value).build();
	}

	/**
	 * Lambda 알림 설정을 만든다.
	 * events 가 null 이면 s3:ObjectCreated:* 를 쓰고, filterRules 가 null 이면 필터를 넣지 않는다.
	 */
	private LambdaFunctionConfiguration lambdaConfig(String id, List<String> events, List<FilterRule> filterRules) {
		var builder = LambdaFunctionConfiguration.builder().id(id).lambdaFunctionArn(lambdaFunctionArn())
				.eventsWithStrings(events == null ? List.of("s3:ObjectCreated:*") : events);
		if (filterRules != null)
			builder.filter(f -> f.key(k -> k.filterRules(filterRules)));
		return builder.build();
	}

	private static NotificationConfiguration lambdaNotification(LambdaFunctionConfiguration... lambdas) {
		return NotificationConfiguration.builder().lambdaFunctionConfigurations(lambdas).build();
	}
}
