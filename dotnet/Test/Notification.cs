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
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.Net;
using Xunit;

namespace s3tests.Test
{
	public class Notification : TestBase
	{
		public Notification(Xunit.Abstractions.ITestOutputHelper Output) => this.Output = Output;

		private static readonly List<EventType> S3Events =
		[
			EventType.ObjectCreatedAll,
			EventType.ObjectRemovedAll
		];

		private static List<LambdaFunctionConfiguration> BuildLambdaConfigs(string roleId, string functionArn)
		{
			return
			[
				new LambdaFunctionConfiguration
				{
					Id = roleId,
					FunctionArn = functionArn,
					Events = S3Events
				}
			];
		}

		/// <summary>
		/// Lambda 함수 ARN을 정식 형식(arn:aws:lambda:리전:계정:function:이름)으로 만든다.
		/// AWS는 형식이 틀린 ARN을 400 InvalidArgument 로 거부한다.
		/// </summary>
		private string LambdaFunctionArn()
		{
			var region = string.IsNullOrWhiteSpace(Config.S3.RegionName) ? "us-east-1" : Config.S3.RegionName;
			return "arn:aws:lambda:" + region + ":" + Config.MainUser.ExpectedOwnerId + ":function:my-function";
		}

		[Fact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "빈 버킷 알림 설정 조회")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestNotificationGetEmpty()
		{
			TestId = 1;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var result = client.GetBucketNotificationConfiguration(bucketName);

			Assert.Empty(result.LambdaFunctionConfigurations ?? []);
			Assert.Empty(result.QueueConfigurations ?? []);
			Assert.Empty(result.TopicConfigurations ?? []);
		}

		[Fact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "Put")]
		[Trait(MainData.Explanation, "버킷 알림 설정")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestNotificationPut()
		{
			TestId = 2;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var roleId = "my-lambda";
			var functionArn = LambdaFunctionArn();

			PutNotification(client, bucketName,
				lambdaFunctionConfigurations: BuildLambdaConfigs(roleId, functionArn));
		}

		[Fact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "Get")]
		[Trait(MainData.Explanation, "버킷 알림 설정 조회")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestNotificationGet()
		{
			TestId = 3;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var roleId = "my-lambda";
			var functionArn = LambdaFunctionArn();

			PutNotification(client, bucketName,
				lambdaFunctionConfigurations: BuildLambdaConfigs(roleId, functionArn));

			var result = client.GetBucketNotificationConfiguration(bucketName);
			var resultLambda = result.LambdaFunctionConfigurations[0];
			S3EventCompare(S3Events, resultLambda.Events);
		}

		[Fact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "Delete")]
		[Trait(MainData.Explanation, "버킷 알림 설정 삭제")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestNotificationDelete()
		{
			TestId = 4;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var roleId = "my-lambda";
			var functionArn = LambdaFunctionArn();

			PutNotification(client, bucketName,
				lambdaFunctionConfigurations: BuildLambdaConfigs(roleId, functionArn));

			var result = client.GetBucketNotificationConfiguration(bucketName);
			S3EventCompare(S3Events, result.LambdaFunctionConfigurations[0].Events);

			client.PutBucketNotificationConfiguration(bucketName,
				lambdaFunctionConfigurations: []);

			var deleteResult = client.GetBucketNotificationConfiguration(bucketName);
			Assert.Empty(deleteResult.LambdaFunctionConfigurations ?? []);
		}

		[Fact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "형식이 잘못된 Lambda ARN으로 알림 설정 시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestNotificationPutInvalidArn()
		{
			TestId = 5;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			// arn: 접두어와 리전이 없는 잘못된 형식
			var functionArn = "aws:lambda::" + Config.MainUser.UserId + ":function:my-function";

			var e = Assert.Throws<AggregateException>(()
				=> PutNotification(client, bucketName,
					lambdaFunctionConfigurations: BuildLambdaConfigs("my-lambda", functionArn)));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_ARGUMENT, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "EventBridge")]
		[Trait(MainData.Explanation, "버킷에 EventBridge 알림 설정, 조회, 삭제가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestNotificationEventBridge()
		{
			TestId = 6;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			// EventBridge 는 대상 리소스가 없어 AWS 에서도 그대로 설정된다
			client.PutBucketNotificationConfiguration(bucketName,
				eventBridgeConfiguration: new EventBridgeConfiguration());

			var result = client.GetBucketNotificationConfiguration(bucketName);
			Assert.NotNull(result.EventBridgeConfiguration);
			Assert.Empty(result.LambdaFunctionConfigurations ?? []);

			client.PutBucketNotificationConfiguration(bucketName,
				lambdaFunctionConfigurations: []);

			var deleteResult = client.GetBucketNotificationConfiguration(bucketName);
			Assert.Null(deleteResult.EventBridgeConfiguration);
		}

		/// <summary>
		/// 대상 리소스 검증을 건너뛰고 알림 설정을 저장한다 (x-amz-skip-destination-validation).
		/// </summary>
		private static void PutNotification(S3Client client, string bucketName,
			List<LambdaFunctionConfiguration> lambdaFunctionConfigurations = null,
			List<TopicConfiguration> topicConfigurations = null,
			List<QueueConfiguration> queueConfigurations = null)
		{
			client.PutBucketNotificationConfiguration(bucketName,
				lambdaFunctionConfigurations: lambdaFunctionConfigurations,
				topicConfigurations: topicConfigurations,
				queueConfigurations: queueConfigurations,
				skipDestinationValidation: true);
		}

		private string Arn(string service, string resource)
		{
			var region = string.IsNullOrWhiteSpace(Config.S3.RegionName) ? "us-east-1" : Config.S3.RegionName;
			return "arn:aws:" + service + ":" + region + ":" + Config.MainUser.ExpectedOwnerId + ":" + resource;
		}

		private static List<FilterRule> PrefixRule(string prefix) => [new FilterRule { Name = "prefix", Value = prefix }];

		private LambdaFunctionConfiguration LambdaConfig(string configId = "my-lambda",
			List<EventType> events = null, List<FilterRule> filterRules = null)
		{
			var config = new LambdaFunctionConfiguration
			{
				Id = configId,
				FunctionArn = LambdaFunctionArn(),
				Events = events ?? [EventType.ObjectCreatedAll],
			};
			if (filterRules != null)
				config.Filter = new Filter { S3KeyFilter = new S3KeyFilter { FilterRules = filterRules } };
			return config;
		}

		[Fact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "Queue")]
		[Trait(MainData.Explanation, "버킷에 SQS 큐 알림 설정, 조회, 삭제가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestNotificationQueue()
		{
			TestId = 7;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var queueArn = Arn("sqs", "my-queue");

			PutNotification(client, bucketName,
				queueConfigurations: [new QueueConfiguration { Id = "my-queue", Queue = queueArn, Events = S3Events }]);

			var result = client.GetBucketNotificationConfiguration(bucketName);
			Assert.Single(result.QueueConfigurations);
			Assert.Equal("my-queue", result.QueueConfigurations[0].Id);
			Assert.Equal(queueArn, result.QueueConfigurations[0].Queue);
			S3EventCompare(S3Events, result.QueueConfigurations[0].Events);

			PutNotification(client, bucketName);
			var deleteResult = client.GetBucketNotificationConfiguration(bucketName);
			Assert.Empty(deleteResult.QueueConfigurations ?? []);
		}

		[Fact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "Topic")]
		[Trait(MainData.Explanation, "버킷에 SNS 토픽 알림 설정, 조회, 삭제가 가능한지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestNotificationTopic()
		{
			TestId = 8;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			var topicArn = Arn("sns", "my-topic");

			PutNotification(client, bucketName,
				topicConfigurations: [new TopicConfiguration { Id = "my-topic", Topic = topicArn, Events = S3Events }]);

			var result = client.GetBucketNotificationConfiguration(bucketName);
			Assert.Single(result.TopicConfigurations);
			Assert.Equal("my-topic", result.TopicConfigurations[0].Id);
			Assert.Equal(topicArn, result.TopicConfigurations[0].Topic);
			S3EventCompare(S3Events, result.TopicConfigurations[0].Events);

			PutNotification(client, bucketName);
			var deleteResult = client.GetBucketNotificationConfiguration(bucketName);
			Assert.Empty(deleteResult.TopicConfigurations ?? []);
		}

		[Fact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "Filter")]
		[Trait(MainData.Explanation, "알림 설정의 접두어/접미어 필터가 저장되고 조회되는지 확인")]
		[Trait(MainData.Result, MainData.ResultSuccess)]
		public void TestNotificationFilter()
		{
			TestId = 9;
			var client = GetClient();
			var bucketName = GetNewBucket(client);
			List<FilterRule> rules =
			[
				new FilterRule { Name = "prefix", Value = "logs/" },
				new FilterRule { Name = "suffix", Value = ".txt" },
			];

			PutNotification(client, bucketName, lambdaFunctionConfigurations: [LambdaConfig(filterRules: rules)]);

			var result = client.GetBucketNotificationConfiguration(bucketName);
			var resultRules = result.LambdaFunctionConfigurations[0].Filter.S3KeyFilter.FilterRules;
			// AWS 는 필터 이름을 Prefix/Suffix 로 정규화해 돌려주므로 대소문자를 무시하고 비교한다
			var actual = new List<string>();
			foreach (var rule in resultRules) actual.Add(rule.Name.ToLowerInvariant() + "=" + rule.Value);
			actual.Sort(string.CompareOrdinal);
			Assert.Equal(["prefix=logs/", "suffix=.txt"], actual);
		}

		[Fact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "대상 검증을 건너뛰지 않으면 존재하지 않는 대상으로 알림 설정 시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestNotificationDestinationNotExist()
		{
			TestId = 10;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			// 대상 검증을 건너뛰지 않으면 존재하지 않는 대상은 거부된다
			var e = Assert.Throws<AggregateException>(()
				=> client.PutBucketNotificationConfiguration(bucketName,
					lambdaFunctionConfigurations: [LambdaConfig()]));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_ARGUMENT, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "지원하지 않는 이벤트 이름으로 알림 설정 시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestNotificationInvalidEvent()
		{
			TestId = 11;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var e = Assert.Throws<AggregateException>(()
				=> PutNotification(client, bucketName,
					lambdaFunctionConfigurations: [LambdaConfig(events: [new EventType("s3:Foo:*")])]));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_ARGUMENT, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "잘못된 필터 이름으로 알림 설정 시 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestNotificationInvalidFilterName()
		{
			TestId = 12;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var e = Assert.Throws<AggregateException>(()
				=> PutNotification(client, bucketName,
					lambdaFunctionConfigurations: [LambdaConfig(filterRules: [new FilterRule { Name = "foo", Value = "a" }])]));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_ARGUMENT, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "같은 이벤트를 공유하는 설정은 필터가 겹치면 실패하고 겹치지 않으면 성공하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestNotificationOverlappingEvent()
		{
			TestId = 13;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			// 필터 없이 같은 이벤트를 두 설정이 공유하면 거부된다
			var e = Assert.Throws<AggregateException>(()
				=> PutNotification(client, bucketName,
					lambdaFunctionConfigurations: [LambdaConfig("c1"), LambdaConfig("c2")]));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_ARGUMENT, GetErrorCode(e));

			// 접두어가 겹치지 않으면 같은 이벤트라도 허용된다
			PutNotification(client, bucketName,
				lambdaFunctionConfigurations:
				[
					LambdaConfig("c1", filterRules: PrefixRule("a/")),
					LambdaConfig("c2", filterRules: PrefixRule("b/")),
				]);

			var result = client.GetBucketNotificationConfiguration(bucketName);
			var ids = new List<string>();
			foreach (var config in result.LambdaFunctionConfigurations) ids.Add(config.Id);
			ids.Sort(string.CompareOrdinal);
			Assert.Equal(["c1", "c2"], ids);
		}

		[Fact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "같은 Id 를 가진 알림 설정이 중복되면 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestNotificationDuplicateId()
		{
			TestId = 14;
			var client = GetClient();
			var bucketName = GetNewBucket(client);

			var e = Assert.Throws<AggregateException>(()
				=> PutNotification(client, bucketName,
					lambdaFunctionConfigurations:
					[
						LambdaConfig("c1", filterRules: PrefixRule("a/")),
						LambdaConfig("c1", filterRules: PrefixRule("b/")),
					]));
			Assert.Equal(HttpStatusCode.BadRequest, GetStatus(e));
			Assert.Equal(MainData.INVALID_ARGUMENT, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "Error")]
		[Trait(MainData.Explanation, "존재하지 않는 버킷의 알림 설정 조회, 저장이 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestNotificationBucketNotExist()
		{
			TestId = 15;
			var client = GetClient();
			var bucketName = GetNewBucketName(false);

			var e = Assert.Throws<AggregateException>(() => client.GetBucketNotificationConfiguration(bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));

			e = Assert.Throws<AggregateException>(() => PutNotification(client, bucketName));
			Assert.Equal(HttpStatusCode.NotFound, GetStatus(e));
			Assert.Equal(MainData.NO_SUCH_BUCKET, GetErrorCode(e));
		}

		[Fact]
		[Trait(MainData.Major, "Notification")]
		[Trait(MainData.Minor, "Permission")]
		[Trait(MainData.Explanation, "권한이 없는 사용자의 알림 설정 조회, 저장이 실패하는지 확인")]
		[Trait(MainData.Result, MainData.ResultFailure)]
		public void TestNotificationAltUser()
		{
			TestId = 16;
			var client = GetClient();
			var altClient = GetAltClient();
			var bucketName = GetNewBucket(client);

			var e = Assert.Throws<AggregateException>(() => altClient.GetBucketNotificationConfiguration(bucketName));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));

			e = Assert.Throws<AggregateException>(() => PutNotification(altClient, bucketName));
			Assert.Equal(HttpStatusCode.Forbidden, GetStatus(e));
			Assert.Equal(MainData.ACCESS_DENIED, GetErrorCode(e));
		}
	}
}
