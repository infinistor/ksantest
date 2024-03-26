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
	// 버킷에 알람 설정이 없는지 확인
	public void test_notification_get_empty() {
		var bucketName = getNewBucket();
		var client = getClient();

		var result = client.getBucketNotificationConfiguration(bucketName);

		assert result.getConfigurations().size() == 0;
	}

	@Test
	@Tag("Put")
	// 버킷에 알람 설정이 가능한지 확인
	public void test_notification_put() {
		var bucketName = getNewBucket();
		var client = getClient();
		var mainUserId = config.mainUser.userId;
		var functionARN = "aws:lambda::" + mainUserId + ":function:my-function";

		var s3Events = EnumSet.noneOf(S3Event.class);
		s3Events.add(S3Event.ObjectCreated);
		s3Events.add(S3Event.ObjectRemoved);
		// 알람 설정
		var notification = new BucketNotificationConfiguration();
		var lambdaFunction = new LambdaConfiguration(functionARN, s3Events);
		notification.addConfiguration("my-lambda", lambdaFunction);

		var request = new SetBucketNotificationConfigurationRequest(bucketName, notification);

		client.setBucketNotificationConfiguration(request);
	}

	@Test
	@Tag("Get")
	// 버킷에 알람 설정이 되어있는지 확인
	public void test_notification_get() {
		var bucketName = getNewBucket();
		var client = getClient();
		var mainUserId = config.mainUser.userId;
		var functionARN = "aws:lambda::" + mainUserId + ":function:my-function";

		var s3Events = EnumSet.noneOf(S3Event.class);
		s3Events.add(S3Event.ObjectCreated);
		s3Events.add(S3Event.ObjectRemoved);
		// 알람 설정
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
	// 버킷에 알람 설정이 삭제되는지 확인
	public void test_notification_delete() {
		var bucketName = getNewBucket();
		var client = getClient();
		var mainUserId = config.mainUser.userId;
		var functionARN = "aws:lambda::" + mainUserId + ":function:my-function";

		var s3Events = EnumSet.noneOf(S3Event.class);
		s3Events.add(S3Event.ObjectCreated);
		s3Events.add(S3Event.ObjectRemoved);
		// 알람 설정
		var notification = new BucketNotificationConfiguration();
		var lambdaFunction = new LambdaConfiguration(functionARN, s3Events);
		notification.addConfiguration("my-lambda", lambdaFunction);

		var request = new SetBucketNotificationConfigurationRequest(bucketName, notification);

		client.setBucketNotificationConfiguration(request);

		var result = client.getBucketNotificationConfiguration(bucketName);
		var resultLambda = result.getConfigurationByName("my-lambda");
		s3eventCompare(s3Events.toArray(), resultLambda.getEvents().toArray());

		// 알람 삭제
		var deleteNotification = new BucketNotificationConfiguration();

		var deleteRequest = new SetBucketNotificationConfigurationRequest(bucketName, deleteNotification);

		client.setBucketNotificationConfiguration(deleteRequest);

		var deleteResult = client.getBucketNotificationConfiguration(bucketName);
		var deleteResultLambda = deleteResult.getConfigurationByName("my-lambda");

		assert deleteResultLambda == null;
	}
}
