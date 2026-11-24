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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.hc.core5.http.HttpStatus;
import org.example.Data.MainData;
import org.junit.Test;
import org.junit.jupiter.api.Tag;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.BucketNotificationConfiguration;
import com.amazonaws.services.s3.model.EventBridgeConfiguration;
import com.amazonaws.services.s3.model.Filter;
import com.amazonaws.services.s3.model.FilterRule;
import com.amazonaws.services.s3.model.LambdaConfiguration;
import com.amazonaws.services.s3.model.QueueConfiguration;
import com.amazonaws.services.s3.model.S3Event;
import com.amazonaws.services.s3.model.S3KeyFilter;
import com.amazonaws.services.s3.model.SetBucketNotificationConfigurationRequest;
import com.amazonaws.services.s3.model.TopicConfiguration;

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
		var client = getClient();
		var bucketName = createBucket(client, 2);
		var functionARN = lambdaFunctionArn();

		var s3Events = EnumSet.noneOf(S3Event.class);
		s3Events.add(S3Event.ObjectCreated);
		s3Events.add(S3Event.ObjectRemoved);
		var notification = new BucketNotificationConfiguration();
		var lambdaFunction = new LambdaConfiguration(functionARN, s3Events);
		notification.addConfiguration("my-lambda", lambdaFunction);

		putNotification(client, bucketName, notification);
	}

	@Test
	@Tag("Get")
	public void testNotificationGet() {
		var client = getClient();
		var bucketName = createBucket(client, 3);
		var functionARN = lambdaFunctionArn();

		var s3Events = EnumSet.noneOf(S3Event.class);
		s3Events.add(S3Event.ObjectCreated);
		s3Events.add(S3Event.ObjectRemoved);
		var notification = new BucketNotificationConfiguration();
		var lambdaFunction = new LambdaConfiguration(functionARN, s3Events);
		notification.addConfiguration("my-lambda", lambdaFunction);

		putNotification(client, bucketName, notification);

		var result = client.getBucketNotificationConfiguration(bucketName);
		var resultLambda = result.getConfigurationByName("my-lambda");

		s3eventCompare(s3Events.toArray(), resultLambda.getEvents().toArray());
	}

	@Test
	@Tag("Delete")
	public void testNotificationDelete() {
		var client = getClient();
		var bucketName = createBucket(client, 4);
		var functionARN = lambdaFunctionArn();

		var s3Events = EnumSet.noneOf(S3Event.class);
		s3Events.add(S3Event.ObjectCreated);
		s3Events.add(S3Event.ObjectRemoved);
		var notification = new BucketNotificationConfiguration();
		var lambdaFunction = new LambdaConfiguration(functionARN, s3Events);
		notification.addConfiguration("my-lambda", lambdaFunction);

		putNotification(client, bucketName, notification);

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

		var e = assertThrows(AmazonServiceException.class, () -> putNotification(client, bucketName, notification));
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

	@Test
	@Tag("Queue")
	public void testNotificationQueue() {
		var client = getClient();
		var bucketName = createBucket(client, 7);
		var queueArn = arn("sqs", "my-queue");
		var s3Events = EnumSet.of(S3Event.ObjectCreated, S3Event.ObjectRemoved);

		var notification = new BucketNotificationConfiguration();
		notification.addConfiguration("my-queue", new QueueConfiguration(queueArn, s3Events));
		putNotification(client, bucketName, notification);

		var result = client.getBucketNotificationConfiguration(bucketName);
		assertEquals(1, result.getConfigurations().size());
		var resultQueue = (QueueConfiguration) result.getConfigurationByName("my-queue");
		assertNotNull(resultQueue);
		assertEquals(queueArn, resultQueue.getQueueARN());
		s3eventCompare(s3Events.toArray(), resultQueue.getEvents().toArray());

		putNotification(client, bucketName, new BucketNotificationConfiguration());
		var deleteResult = client.getBucketNotificationConfiguration(bucketName);
		assertEquals(0, deleteResult.getConfigurations().size());
	}

	@Test
	@Tag("Topic")
	public void testNotificationTopic() {
		var client = getClient();
		var bucketName = createBucket(client, 8);
		var topicArn = arn("sns", "my-topic");
		var s3Events = EnumSet.of(S3Event.ObjectCreated, S3Event.ObjectRemoved);

		var notification = new BucketNotificationConfiguration();
		notification.addConfiguration("my-topic", new TopicConfiguration(topicArn, s3Events));
		putNotification(client, bucketName, notification);

		var result = client.getBucketNotificationConfiguration(bucketName);
		assertEquals(1, result.getConfigurations().size());
		var resultTopic = (TopicConfiguration) result.getConfigurationByName("my-topic");
		assertNotNull(resultTopic);
		assertEquals(topicArn, resultTopic.getTopicARN());
		s3eventCompare(s3Events.toArray(), resultTopic.getEvents().toArray());

		putNotification(client, bucketName, new BucketNotificationConfiguration());
		var deleteResult = client.getBucketNotificationConfiguration(bucketName);
		assertEquals(0, deleteResult.getConfigurations().size());
	}

	@Test
	@Tag("Filter")
	public void testNotificationFilter() {
		var client = getClient();
		var bucketName = createBucket(client, 9);
		var rules = List.of(filterRule("prefix", "logs/"), filterRule("suffix", ".txt"));

		var notification = new BucketNotificationConfiguration();
		notification.addConfiguration("my-lambda", lambdaConfig(null, rules));
		putNotification(client, bucketName, notification);

		var result = client.getBucketNotificationConfiguration(bucketName);
		var resultRules = result.getConfigurationByName("my-lambda").getFilter().getS3KeyFilter().getFilterRules();
		// AWS 는 필터 이름을 Prefix/Suffix 로 정규화해 돌려주므로 대소문자를 무시하고 비교한다
		var actual = new ArrayList<String>();
		for (var rule : resultRules)
			actual.add(rule.getName().toLowerCase() + "=" + rule.getValue());
		actual.sort(null);
		assertEquals(List.of("prefix=logs/", "suffix=.txt"), actual);
	}

	@Test
	@Tag("Error")
	public void testNotificationInvalidEvent() {
		var client = getClient();
		var bucketName = createBucket(client, 11);
		var notification = new BucketNotificationConfiguration();
		notification.addConfiguration("my-lambda", lambdaConfig(new String[] { "s3:Foo:*" }, null));

		var e = assertThrows(AmazonServiceException.class, () -> putNotification(client, bucketName, notification));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testNotificationInvalidFilterName() {
		var client = getClient();
		var bucketName = createBucket(client, 12);
		var notification = new BucketNotificationConfiguration();
		notification.addConfiguration("my-lambda", lambdaConfig(null, List.of(filterRule("foo", "a"))));

		var e = assertThrows(AmazonServiceException.class, () -> putNotification(client, bucketName, notification));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.getErrorCode());
	}

	@Test
	@Tag("Error")
	public void testNotificationOverlappingEvent() {
		var client = getClient();
		var bucketName = createBucket(client, 13);

		// 필터 없이 같은 이벤트를 두 설정이 공유하면 거부된다
		var overlapping = new BucketNotificationConfiguration();
		overlapping.addConfiguration("c1", lambdaConfig(null, null));
		overlapping.addConfiguration("c2", lambdaConfig(null, null));
		var e = assertThrows(AmazonServiceException.class, () -> putNotification(client, bucketName, overlapping));
		assertEquals(HttpStatus.SC_BAD_REQUEST, e.getStatusCode());
		assertEquals(MainData.INVALID_ARGUMENT, e.getErrorCode());

		// 접두어가 겹치지 않으면 같은 이벤트라도 허용된다
		var notification = new BucketNotificationConfiguration();
		notification.addConfiguration("c1", lambdaConfig(null, List.of(filterRule("prefix", "a/"))));
		notification.addConfiguration("c2", lambdaConfig(null, List.of(filterRule("prefix", "b/"))));
		putNotification(client, bucketName, notification);

		var result = client.getBucketNotificationConfiguration(bucketName);
		var ids = new ArrayList<>(result.getConfigurations().keySet());
		ids.sort(null);
		assertEquals(List.of("c1", "c2"), ids);
	}

	@Test
	@Tag("Error")
	public void testNotificationBucketNotExist() {
		var client = getClient();
		var bucketName = getNewBucketNameOnly(15);

		var e = assertThrows(AmazonServiceException.class, () -> client.getBucketNotificationConfiguration(bucketName));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class,
				() -> putNotification(client, bucketName, new BucketNotificationConfiguration()));
		assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
		assertEquals(MainData.NO_SUCH_BUCKET, e.getErrorCode());
	}

	@Test
	@Tag("Permission")
	public void testNotificationAltUser() {
		var client = getClient();
		var altClient = getAltClient();
		var bucketName = createBucket(client, 16);

		var e = assertThrows(AmazonServiceException.class,
				() -> altClient.getBucketNotificationConfiguration(bucketName));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());

		e = assertThrows(AmazonServiceException.class,
				() -> putNotification(altClient, bucketName, new BucketNotificationConfiguration()));
		assertEquals(HttpStatus.SC_FORBIDDEN, e.getStatusCode());
		assertEquals(MainData.ACCESS_DENIED, e.getErrorCode());
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
	private void putNotification(AmazonS3 client, String bucketName, BucketNotificationConfiguration notification) {
		client.setBucketNotificationConfiguration(
				new SetBucketNotificationConfigurationRequest(bucketName, notification)
						.withSkipDestinationValidation(true));
	}

	private String arn(String service, String resource) {
		var region = (config.regionName == null || config.regionName.isBlank()) ? "us-east-1" : config.regionName;
		return "arn:aws:" + service + ":" + region + ":" + config.mainUser.getExpectedOwnerId() + ":" + resource;
	}

	private static FilterRule filterRule(String name, String value) {
		return new FilterRule().withName(name).withValue(value);
	}

	/**
	 * Lambda 알림 설정을 만든다.
	 * events 가 null 이면 s3:ObjectCreated:* 를 쓰고, filterRules 가 null 이면 필터를 넣지 않는다.
	 */
	private LambdaConfiguration lambdaConfig(String[] events, List<FilterRule> filterRules) {
		var lambda = new LambdaConfiguration(lambdaFunctionArn(),
				events == null ? new String[] { "s3:ObjectCreated:*" } : events);
		if (filterRules != null)
			lambda.setFilter(new Filter().withS3KeyFilter(new S3KeyFilter().withFilterRules(filterRules)));
		return lambda;
	}
}
